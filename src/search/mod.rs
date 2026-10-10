//! Full-text search backed by `tantivy`.
//!
//! Phase 3 gives every partition worker an inverted index living in a
//! RAM directory, so searching never touches the disk. The index is
//! *hot*: the storage worker adds/removes documents as they arrive,
//! keeping the transactional map and the search index consistent.
//!
//! Per `docs/CONTRACT.md`, this module never sees raw BSON. The
//! storage layer extracts plain strings ([`crate::types::SearchHit`]
//! is the only result type leaving this module) and hands them over.
//!
//! Scoring is BM25 (tantivy's default). When the exact query matches
//! nothing, the search falls back to a fuzzy (typo-tolerant) query so
//! a one-character mistake still finds the document.

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, Value, STORED, STRING, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter, TantivyDocument, Term};

use crate::types::SearchHit;

/// Memory budget (bytes) reserved for one partition's index writer.
///
/// Must be a power of two (tantivy requirement). 16 MiB per partition
/// keeps four partitions well under 64 MiB of indexing overhead.
const WRITER_MEMORY_BUDGET: usize = 1 << 24;

/// Default number of hits returned by one search.
const DEFAULT_LIMIT: usize = 100;

/// Maximum edit distance tolerated by the fuzzy fallback.
const FUZZY_DISTANCE: u8 = 1;

/// Schema field handles, resolved once when the index is built.
#[derive(Clone, Copy)]
struct Fields {
    /// Partition-owning collection name (untokenized, used as filter).
    collection: Field,
    /// Storage-side document id (untokenized, stored for read-back).
    doc_id: Field,
    /// `collection:doc_id` pair; the unique key tantivy deletes by.
    unique_key: Field,
    /// Concatenated text of the document (tokenized, BM25-scored).
    text: Field,
}

/// An in-memory inverted index owned by one partition worker.
///
/// The index is confined to the worker thread (thread-per-core), so no
/// lock guards it. The [`IndexWriter`] is kept alive for the lifetime
/// of the partition: dropping it would flush and close the index.
pub struct TextIndex {
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    fields: Fields,
}

impl TextIndex {
    /// Creates an empty index in a RAM directory.
    pub fn new() -> Result<Self, String> {
        let mut builder = Schema::builder();
        let collection = builder.add_text_field("collection", STRING);
        let doc_id = builder.add_text_field("doc_id", STRING | STORED);
        let unique_key = builder.add_text_field("unique_key", STRING);
        let text = builder.add_text_field("text", TEXT);
        let schema = builder.build();

        let index = Index::create_in_ram(schema);
        // One indexing thread: the partition worker drives indexing
        // from its own thread, so extra threads would only contend.
        let writer = index
            .writer_with_num_threads(1, WRITER_MEMORY_BUDGET)
            .map_err(|err| format!("failed to create search writer: {}", err))?;
        let reader = index
            .reader()
            .map_err(|err| format!("failed to create search reader: {}", err))?;

        Ok(Self {
            index,
            writer,
            reader,
            fields: Fields {
                collection,
                doc_id,
                unique_key,
                text,
            },
        })
    }

    /// Indexes (or re-indexes) one document's text.
    ///
    /// Callers replacing a document must [`Self::remove`] it first;
    /// tantivy does not deduplicate by unique key on insert.
    pub fn add(&mut self, collection: &str, doc_id: &str, text: &str) -> Result<(), String> {
        let document = doc!(
            self.fields.collection => collection,
            self.fields.doc_id => doc_id,
            self.fields.unique_key => unique_key(collection, doc_id),
            self.fields.text => text,
        );
        self.writer
            .add_document(document)
            .map(|_| ())
            .map_err(|err| format!("failed to add document to index: {}", err))
    }

    /// Removes one document from the index.
    pub fn remove(&mut self, collection: &str, doc_id: &str) -> Result<(), String> {
        // `delete_term` queues the deletion; it is applied on commit.
        self.writer.delete_term(Term::from_field_text(
            self.fields.unique_key,
            &unique_key(collection, doc_id),
        ));
        Ok(())
    }

    /// Removes every document of a collection (used by `drop`).
    pub fn remove_collection(&mut self, collection: &str) -> Result<(), String> {
        self.writer
            .delete_term(Term::from_field_text(self.fields.collection, collection));
        Ok(())
    }

    /// Searches one collection and returns document ids by relevance.
    ///
    /// Commits pending writes first so freshly inserted documents are
    /// visible to the query (searches are not on the hot path). When
    /// the exact query scores zero hits, the search retries with a
    /// fuzzy query to tolerate typos.
    pub fn search(&mut self, collection: &str, query: &str) -> Result<Vec<SearchHit>, String> {
        self.writer
            .commit()
            .map_err(|err| format!("search commit failed: {}", err))?;
        // Reload synchronously: freshly inserted documents must be
        // visible to this search, not to the next delayed refresh.
        self.reader
            .reload()
            .map_err(|err| format!("search reload failed: {}", err))?;
        let searcher = self.reader.searcher();
        let limit = DEFAULT_LIMIT;

        let exact = self.query(collection, query, false);
        let mut hits = run_search(&searcher, &exact, self.fields.doc_id, limit)?;

        if hits.is_empty() && !query.trim().is_empty() {
            let fuzzy = self.query(collection, query, true);
            hits = run_search(&searcher, &fuzzy, self.fields.doc_id, limit)?;
        }

        Ok(hits)
    }

    /// Builds the query for `collection`, optionally fuzzy.
    fn query(&self, collection: &str, query: &str, fuzzy: bool) -> Box<dyn Query> {
        let collection_clause = (
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(self.fields.collection, collection),
                IndexRecordOption::Basic,
            )) as Box<dyn Query>,
        );

        let text_clause = if fuzzy {
            fuzzy_query(self.fields.text, query)
        } else {
            match QueryParser::for_index(&self.index, vec![self.fields.text]).parse_query(query) {
                Ok(parsed) => parsed,
                // Unparseable input (unbalanced quotes, stray `$`) still
                // deserves an answer: degrade to fuzzy matching.
                Err(_) => fuzzy_query(self.fields.text, query),
            }
        };

        Box::new(BooleanQuery::from(vec![
            collection_clause,
            (Occur::Must, text_clause),
        ]))
    }
}

/// Builds the tantivy-unique key for one document.
fn unique_key(collection: &str, doc_id: &str) -> String {
    format!("{}\u{0}{}", collection, doc_id)
}

/// Builds an OR-of-fuzzy-terms query.
///
/// The input is split on any non-alphanumeric character, so both a
/// typo (`databse`) and input that broke the strict query parser
/// (`"compiler`) degrade into a term match. Each term tolerates
/// [`FUZZY_DISTANCE`] edit(s).
fn fuzzy_query(field: Field, query: &str) -> Box<dyn Query> {
    let clauses: Vec<(Occur, Box<dyn Query>)> = query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| {
            (
                Occur::Should,
                Box::new(FuzzyTermQuery::new(
                    Term::from_field_text(field, term),
                    FUZZY_DISTANCE,
                    true,
                )) as Box<dyn Query>,
            )
        })
        .collect();

    if clauses.is_empty() {
        // No terms to match: an empty boolean query matches nothing,
        // which is the right answer for a query of pure punctuation.
        Box::new(BooleanQuery::from(vec![]))
    } else {
        Box::new(BooleanQuery::from(clauses))
    }
}

/// Runs a query and maps hits to [`SearchHit`]s in relevance order.
fn run_search(
    searcher: &tantivy::Searcher,
    query: &Box<dyn Query>,
    doc_id_field: Field,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let top = searcher
        .search(query, &TopDocs::with_limit(limit.max(1)))
        .map_err(|err| format!("search failed: {}", err))?;

    let mut hits = Vec::with_capacity(top.len());
    // TopDocs yields (score, address); the score is implicit in the
    // ordering, which is all the worker needs to rank the reply.
    for (_score, address) in top {
        let document: TantivyDocument = searcher
            .doc(address)
            .map_err(|err| format!("failed to read search hit: {}", err))?;
        if let Some(id) = document
            .get_first(doc_id_field)
            .and_then(|value| value.as_str())
        {
            hits.push(SearchHit { id: id.to_string() });
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_with_users() -> TextIndex {
        let mut index = TextIndex::new().expect("index");
        index
            .add("users", "1", "Ada wrote the first computer program")
            .expect("add");
        index
            .add("users", "2", "Grace designed the first compiler")
            .expect("add");
        index
            .add("orders", "3", "Ada ordered a database book")
            .expect("add");
        index.commit_for_test().expect("commit");
        index
    }

    impl TextIndex {
        /// Test-only commit so fixtures are searchable without a query.
        fn commit_for_test(&mut self) -> Result<(), String> {
            self.writer
                .commit()
                .map(|_| ())
                .map_err(|err| format!("commit failed: {}", err))
        }
    }

    fn ids(hits: &[SearchHit]) -> Vec<&str> {
        hits.iter().map(|hit| hit.id.as_str()).collect()
    }

    #[test]
    fn test_search_finds_matching_documents() {
        let mut index = index_with_users();
        let hits = index.search("users", "compiler").expect("search");
        assert_eq!(ids(&hits), vec!["2"]);
    }

    #[test]
    fn test_search_is_scoped_to_one_collection() {
        let mut index = index_with_users();
        // "database" only appears in the orders collection.
        let hits = index.search("users", "database").expect("search");
        assert!(hits.is_empty(), "users must not match orders: {:?}", hits);

        let hits = index.search("orders", "database").expect("search");
        assert_eq!(ids(&hits), vec!["3"]);
    }

    #[test]
    fn test_search_orders_by_bm25_relevance() {
        let mut index = TextIndex::new().expect("index");
        // "rust" appears twice in doc "a" and once in doc "b".
        index.add("c", "a", "rust rust engine fast").expect("add");
        index.add("c", "b", "rust is fast").expect("add");
        index.commit_for_test().expect("commit");

        let hits = index.search("c", "rust").expect("search");
        assert_eq!(
            ids(&hits),
            vec!["a", "b"],
            "more matches must rank first (BM25 order)"
        );
    }

    #[test]
    fn test_fuzzy_fallback_tolerates_typos() {
        let mut index = index_with_users();
        // "compier" is one edit away from "compiler".
        let hits = index.search("users", "compier").expect("search");
        assert_eq!(ids(&hits), vec!["2"], "typo must still match");
    }

    #[test]
    fn test_remove_hides_document_from_search() {
        let mut index = index_with_users();
        index.remove("users", "2").expect("remove");
        index.commit_for_test().expect("commit");

        let hits = index.search("users", "compiler").expect("search");
        assert!(hits.is_empty(), "removed doc must not match: {:?}", hits);
    }

    #[test]
    fn test_remove_collection_hides_only_that_collection() {
        let mut index = index_with_users();
        index.remove_collection("users").expect("drop");

        assert!(index
            .search("users", "compiler")
            .expect("search")
            .is_empty());
        assert_eq!(
            ids(&index.search("orders", "database").expect("search")),
            vec!["3"]
        );
    }

    #[test]
    fn test_search_returns_empty_for_unknown_collection() {
        let mut index = index_with_users();
        assert!(index.search("nothing", "rust").expect("search").is_empty());
    }

    #[test]
    fn test_search_sees_uncommitted_inserts() {
        // `search` commits before querying, so a document added in the
        // same call sequence must be visible without an explicit commit.
        let mut index = TextIndex::new().expect("index");
        index.add("c", "1", "hot path").expect("add");
        let hits = index.search("c", "hot").expect("search");
        assert_eq!(ids(&hits), vec!["1"]);
    }

    #[test]
    fn test_unparseable_query_falls_back_instead_of_failing() {
        let mut index = index_with_users();
        // A leading `$` and unbalanced quote break QueryParser syntax.
        let hits = index.search("users", "\"compiler").expect("search");
        assert_eq!(ids(&hits), vec!["2"]);
    }
}

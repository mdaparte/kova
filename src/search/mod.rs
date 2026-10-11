//! Full-text search backed by `tantivy`.
//!
//! Phase 3 gave every partition worker an inverted index living in a
//! RAM directory, so searching never touches the disk. Phase 3.5
//! refined it into a *per-field* index: one tantivy index per document
//! field name, which is what makes MongoDB-style field boosts
//! (`$text.$boost`) possible — each field keeps its own BM25 corpus
//! and statistics, and the merge sums the weighted scores.
//!
//! Per `docs/CONTRACT.md`, this module never sees raw BSON. The
//! storage layer extracts `(field, text)` pairs and hands them over;
//! [`SearchHit`] is the only result type leaving this module.
//!
//! Scoring is BM25 (tantivy's default). When the exact query matches
//! nothing, the search falls back to a fuzzy (typo-tolerant) query so
//! a one-character mistake still finds the document. Tokens containing
//! `*` or `?` are expanded to anchored regexes (Solr-style wildcards).

use std::collections::{HashMap, HashSet};

use tantivy::collector::TopDocs;
use tantivy::query::{
    BooleanQuery, FuzzyTermQuery, Occur, Query, QueryParser, RegexQuery, TermQuery,
};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, STORED, STRING,
};
use tantivy::tokenizer::{
    Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer,
};
use tantivy::{doc, Index, IndexReader, IndexWriter, TantivyDocument, Term};

use crate::config::CONFIG;
use crate::logger::LOGGER;
use crate::types::SearchHit;

/// Memory budget (bytes) reserved for one field index writer.
///
/// tantivy enforces a minimum arena of ~15 MB per writer thread, so
/// this is the smallest legal power of two above that floor. The
/// budget is the *ceiling*, not an upfront allocation, but it defines
/// the worst-case cost of [`MAX_INDEXED_FIELDS`] indexes.
const WRITER_MEMORY_BUDGET: usize = 1 << 24;

/// Maximum number of distinct field names indexed per partition.
///
/// Every field index owns a writer (>= 15 MB arena), so the cap keeps
/// the worst case bounded: 8 fields x 16 MB = 128 MB per partition,
/// and real documents use far fewer. Fields beyond the cap are
/// skipped with a warning, once per field name.
const MAX_INDEXED_FIELDS: usize = 8;

/// Default number of hits returned by one search.
const DEFAULT_LIMIT: usize = 100;

/// Maximum edit distance tolerated by the fuzzy fallback.
const FUZZY_DISTANCE: u8 = 1;

/// Name the token pipeline is registered under in every index.
///
/// tantivy resolves the tokenizer by name at schema build time; the
/// pipeline itself comes from the `[search]` section of `kova.toml`.
const TOKENIZER_NAME: &str = "kova_text";

/// Every document field, indexed separately.
///
/// One tantivy index per field name is what enables per-field BM25
/// (and therefore `$boost`): scores from independent corpora are not
/// comparable, so they are summed at merge time exactly like
/// Elasticsearch combines multi-field matches.
pub struct SearchIndex {
    /// Field name -> inverted index for that field.
    fields: HashMap<String, TextIndex>,
    /// Analyzer kind (`"default"` / `"en_stem"`) applied to new field indexes.
    analyzer: String,
    /// Stop words removed at index and query time.
    stopwords: Vec<String>,
    /// Fields already reported as skipped (avoids log spam).
    skipped_fields: HashSet<String>,
}

impl SearchIndex {
    /// Creates an empty index set using the configured analyzer.
    pub fn new() -> Self {
        Self::with_analyzer(&CONFIG.search.analyzer, &CONFIG.search.stopwords)
    }

    /// Creates an empty index set with an explicit token pipeline.
    ///
    /// The analyzer is fixed for the lifetime of the index: it is
    /// applied both when indexing and when parsing queries.
    pub fn with_analyzer(analyzer: &str, stopwords: &[String]) -> Self {
        Self {
            fields: HashMap::new(),
            analyzer: analyzer.to_string(),
            stopwords: stopwords.to_vec(),
            skipped_fields: HashSet::new(),
        }
    }

    /// Indexes (or re-indexes) one document's field values.
    ///
    /// Callers replacing a document must [`Self::remove`] it first;
    /// tantivy does not deduplicate by unique key on insert.
    pub fn add(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[(String, String)],
    ) -> Result<(), String> {
        for (name, text) in fields {
            if text.trim().is_empty() {
                continue;
            }

            if !self.fields.contains_key(name) && self.fields.len() >= MAX_INDEXED_FIELDS {
                if self.skipped_fields.insert(name.clone()) {
                    LOGGER.warn(format!(
                        "Search: field limit ({}) reached; skipping field {:?}",
                        MAX_INDEXED_FIELDS, name
                    ));
                }
                continue;
            }

            let index = match self.fields.entry(name.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TextIndex::new(&self.analyzer, &self.stopwords)?)
                }
            };
            index.add(collection, doc_id, text)?;
        }
        Ok(())
    }

    /// Removes one document from every field index (best effort).
    pub fn remove(&mut self, collection: &str, doc_id: &str) -> Result<(), String> {
        for index in self.fields.values_mut() {
            index.remove(collection, doc_id)?;
        }
        Ok(())
    }

    /// Removes every document of a collection from every field index.
    pub fn remove_collection(&mut self, collection: &str) -> Result<(), String> {
        for index in self.fields.values_mut() {
            index.remove_collection(collection)?;
        }
        Ok(())
    }

    /// Searches every indexed field and returns hits by combined score.
    ///
    /// Each field contributes `bm25(field) * boost` per document; a
    /// field absent from `boosts` uses `1.0`, mirroring MongoDB text
    /// index weights (default weight 1). The top [`DEFAULT_LIMIT`]
    /// documents by total score are returned.
    pub fn search(
        &mut self,
        collection: &str,
        query: &str,
        boosts: &[(String, f32)],
    ) -> Result<Vec<SearchHit>, String> {
        let mut scores: HashMap<String, f32> = HashMap::new();

        for (name, index) in &mut self.fields {
            let boost = boosts
                .iter()
                .find(|(field, _)| field == name)
                .map_or(1.0, |(_, boost)| *boost);
            if boost == 0.0 {
                continue;
            }

            for hit in index.search(collection, query)? {
                *scores.entry(hit.id).or_insert(0.0) += hit.score * boost;
            }
        }

        let mut ranked: Vec<SearchHit> = scores
            .into_iter()
            .map(|(id, score)| SearchHit { id, score })
            .collect();
        ranked.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        ranked.truncate(DEFAULT_LIMIT);
        Ok(ranked)
    }
}

/// One tantivy inverted index over a single document field.
///
/// The index is confined to the worker thread (thread-per-core), so no
/// lock guards it. The [`IndexWriter`] is kept alive for the lifetime
/// of the partition: dropping it would flush and close the index.
struct TextIndex {
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    fields: Fields,
}

/// Schema field handles, resolved once when the index is built.
#[derive(Clone, Copy)]
struct Fields {
    /// Partition-owning collection name (untokenized, used as filter).
    collection: Field,
    /// Storage-side document id (untokenized, stored for read-back).
    doc_id: Field,
    /// `collection:doc_id` pair; the unique key tantivy deletes by.
    unique_key: Field,
    /// One field's text (tokenized, BM25-scored).
    text: Field,
}

impl TextIndex {
    /// Creates an empty index in a RAM directory with the given analyzer.
    fn new(analyzer: &str, stopwords: &[String]) -> Result<Self, String> {
        let mut builder = Schema::builder();
        let collection = builder.add_text_field("collection", STRING);
        let doc_id = builder.add_text_field("doc_id", STRING | STORED);
        let unique_key = builder.add_text_field("unique_key", STRING);
        // The text field runs the configured pipeline: the same
        // analyzer must tokenize queries, otherwise indexed and query
        // terms would never line up.
        let text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(TOKENIZER_NAME)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        let text = builder.add_text_field("text", text_options);
        let schema = builder.build();

        let index = Index::create_in_ram(schema);
        index
            .tokenizers()
            .register(TOKENIZER_NAME, build_analyzer(analyzer, stopwords));
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

    /// Indexes (or re-indexes) one document's text for this field.
    fn add(&mut self, collection: &str, doc_id: &str, text: &str) -> Result<(), String> {
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
    fn remove(&mut self, collection: &str, doc_id: &str) -> Result<(), String> {
        // `delete_term` queues the deletion; it is applied on commit.
        self.writer.delete_term(Term::from_field_text(
            self.fields.unique_key,
            &unique_key(collection, doc_id),
        ));
        Ok(())
    }

    /// Removes every document of a collection (used by `drop`).
    fn remove_collection(&mut self, collection: &str) -> Result<(), String> {
        self.writer
            .delete_term(Term::from_field_text(self.fields.collection, collection));
        Ok(())
    }

    /// Searches one collection and returns document ids with scores.
    ///
    /// Commits pending writes first so freshly inserted documents are
    /// visible to the query (searches are not on the hot path). When
    /// the exact query scores zero hits, the search retries with a
    /// fuzzy query to tolerate typos.
    fn search(&mut self, collection: &str, query: &str) -> Result<Vec<SearchHit>, String> {
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
        } else if query.contains('*') || query.contains('?') {
            // Solr-style wildcards go through the terms dictionary as
            // automata instead of the strict query parser.
            wildcard_query(self.fields.text, query)
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

/// Builds the token pipeline for one index.
///
/// `default` splits on punctuation/whitespace and lowercases;
/// `en_stem` adds English stemming for better recall. Configured stop
/// words are appended last so they see lowercased tokens.
fn build_analyzer(kind: &str, stopwords: &[String]) -> TextAnalyzer {
    // `TextAnalyzerBuilder` is typed by its filter chain, so every
    // pipeline combination must be spelled out statically.
    let base = || {
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
    };

    match (kind, stopwords.is_empty()) {
        ("en_stem", true) => base().filter(Stemmer::new(Language::English)).build(),
        ("en_stem", false) => base()
            .filter(Stemmer::new(Language::English))
            .filter(StopWordFilter::remove(stopwords.iter().cloned()))
            .build(),
        (_, true) => base().build(),
        (_, false) => base()
            .filter(StopWordFilter::remove(stopwords.iter().cloned()))
            .build(),
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
                    // Indexed terms are lowercased by the analyzer; the
                    // query term must match that convention.
                    Term::from_field_text(field, &term.to_lowercase()),
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

/// Builds an OR-of-wildcard-clauses query (Solr-style `dat*`, `d?ta`).
///
/// Tokens without wildcard characters are matched exactly. tantivy's
/// automata match the *whole* term, so `*x` cannot match inside an
/// unrelated term — Solr/`WildcardQuery` semantics for free, with no
/// anchors needed (the FST regex dialect rejects `^`/`$`).
fn wildcard_query(field: Field, query: &str) -> Box<dyn Query> {
    let clauses: Vec<(Occur, Box<dyn Query>)> = query
        .split_whitespace()
        .map(|token| {
            if token.contains('*') || token.contains('?') {
                match RegexQuery::from_pattern(&wildcard_to_regex(token), field) {
                    Ok(regex) => (Occur::Should, Box::new(regex) as Box<dyn Query>),
                    // Invalid automaton (pathological pattern): fall
                    // back to an exact term so the query still answers.
                    Err(_) => (
                        Occur::Should,
                        Box::new(TermQuery::new(
                            Term::from_field_text(field, &token.to_lowercase()),
                            IndexRecordOption::WithFreqsAndPositions,
                        )) as Box<dyn Query>,
                    ),
                }
            } else {
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(field, &token.to_lowercase()),
                        IndexRecordOption::WithFreqsAndPositions,
                    )) as Box<dyn Query>,
                )
            }
        })
        .collect();

    if clauses.is_empty() {
        Box::new(BooleanQuery::from(vec![]))
    } else {
        Box::new(BooleanQuery::from(clauses))
    }
}

/// Converts a wildcard token into a lowercased whole-term regex.
///
/// `Dat*` becomes `dat.*`. tantivy matches the automaton against the
/// entire term, so no anchors are required; other regex
/// metacharacters are escaped so user input cannot widen the match
/// (an escaped pattern the FST dialect rejects simply falls back to
/// an exact term in [`wildcard_query`]).
fn wildcard_to_regex(token: &str) -> String {
    let mut pattern = String::with_capacity(token.len() * 2);
    for c in token.chars() {
        match c {
            '*' => pattern.push_str(".*"),
            '?' => pattern.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                pattern.push('\\');
                pattern.push(c);
            }
            other => pattern.extend(other.to_lowercase()),
        }
    }
    pattern
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
    // TopDocs yields (score, address); the score drives the ranking
    // the worker reproduces after the boost merge.
    for (score, address) in top {
        let document: TantivyDocument = searcher
            .doc(address)
            .map_err(|err| format!("failed to read search hit: {}", err))?;
        if let Some(id) = document
            .get_first(doc_id_field)
            .and_then(|value| value.as_str())
        {
            hits.push(SearchHit {
                id: id.to_string(),
                score,
            });
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, text)| (name.to_string(), text.to_string()))
            .collect()
    }

    fn index_with_users() -> SearchIndex {
        let mut index = SearchIndex::new();
        index
            .add(
                "users",
                "1",
                &fields(&[("bio", "Ada wrote the first computer program")]),
            )
            .expect("add");
        index
            .add(
                "users",
                "2",
                &fields(&[("bio", "Grace designed the first compiler")]),
            )
            .expect("add");
        index
            .add(
                "orders",
                "3",
                &fields(&[("item", "Ada ordered a database book")]),
            )
            .expect("add");
        index
    }

    fn ids(hits: &[SearchHit]) -> Vec<&str> {
        hits.iter().map(|hit| hit.id.as_str()).collect()
    }

    #[test]
    fn test_search_finds_matching_documents() {
        let mut index = index_with_users();
        let hits = index.search("users", "compiler", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["2"]);
    }

    #[test]
    fn test_search_is_scoped_to_one_collection() {
        let mut index = index_with_users();
        // "database" only appears in the orders collection.
        let hits = index.search("users", "database", &[]).expect("search");
        assert!(hits.is_empty(), "users must not match orders: {:?}", hits);

        let hits = index.search("orders", "database", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["3"]);
    }

    #[test]
    fn test_search_orders_by_bm25_relevance() {
        let mut index = SearchIndex::new();
        // "rust" appears twice in doc "a" and once in doc "b".
        index
            .add("c", "a", &fields(&[("text", "rust rust engine fast")]))
            .expect("add");
        index
            .add("c", "b", &fields(&[("text", "rust is fast")]))
            .expect("add");

        let hits = index.search("c", "rust", &[]).expect("search");
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
        let hits = index.search("users", "compier", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["2"], "typo must still match");
    }

    #[test]
    fn test_remove_hides_document_from_search() {
        let mut index = index_with_users();
        index.remove("users", "2").expect("remove");

        let hits = index.search("users", "compiler", &[]).expect("search");
        assert!(hits.is_empty(), "removed doc must not match: {:?}", hits);
    }

    #[test]
    fn test_remove_collection_hides_only_that_collection() {
        let mut index = index_with_users();
        index.remove_collection("users").expect("drop");

        assert!(index
            .search("users", "compiler", &[])
            .expect("search")
            .is_empty());
        assert_eq!(
            ids(&index.search("orders", "database", &[]).expect("search")),
            vec!["3"]
        );
    }

    #[test]
    fn test_search_returns_empty_for_unknown_collection() {
        let mut index = index_with_users();
        assert!(index
            .search("nothing", "rust", &[])
            .expect("search")
            .is_empty());
    }

    #[test]
    fn test_search_sees_uncommitted_inserts() {
        // `search` commits before querying, so a document added in the
        // same call sequence must be visible without an explicit commit.
        let mut index = SearchIndex::new();
        index
            .add("c", "1", &fields(&[("text", "hot path")]))
            .expect("add");
        let hits = index.search("c", "hot", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["1"]);
    }

    #[test]
    fn test_unparseable_query_falls_back_instead_of_failing() {
        let mut index = index_with_users();
        // A leading `$` and unbalanced quote break QueryParser syntax.
        let hits = index.search("users", "\"compiler", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["2"]);
    }

    #[test]
    fn test_hits_carry_bm25_scores() {
        let mut index = index_with_users();
        let hits = index.search("users", "compiler", &[]).expect("search");
        assert!(hits.iter().all(|hit| hit.score > 0.0));
    }

    #[test]
    fn test_field_boost_reorders_results() {
        let mut index = SearchIndex::new();
        // Doc "a" mentions rust in the body; doc "b" in the title.
        index
            .add(
                "c",
                "a",
                &fields(&[
                    ("title", "Cooking recipes"),
                    ("body", "rust oven seasoning"),
                ]),
            )
            .expect("add");
        index
            .add(
                "c",
                "b",
                &fields(&[
                    ("title", "Rust programming"),
                    ("body", "the language and compiler"),
                ]),
            )
            .expect("add");

        // Unboosted: BM25 across the two corpora ranks either first
        // depending on field statistics; boost the title 10x and the
        // title match must win decisively.
        let hits = index
            .search("c", "rust", &[("title".to_string(), 10.0)])
            .expect("search");
        assert_eq!(ids(&hits), vec!["b", "a"], "boosted title must rank first");
    }

    #[test]
    fn test_boost_zero_excludes_field() {
        let mut index = SearchIndex::new();
        index
            .add(
                "c",
                "a",
                &fields(&[("title", "rust book"), ("body", "unrelated text")]),
            )
            .expect("add");
        index
            .add(
                "c",
                "b",
                &fields(&[("title", "cooking"), ("body", "rust on the pan")]),
            )
            .expect("add");

        let hits = index
            .search("c", "rust", &[("body".to_string(), 0.0)])
            .expect("search");
        assert_eq!(ids(&hits), vec!["a"], "body matches are excluded");
    }

    #[test]
    fn test_wildcard_prefix_matches_terms() {
        let mut index = index_with_users();
        let hits = index.search("users", "comp*", &[]).expect("search");
        let mut matched = ids(&hits);
        matched.sort_unstable();
        // BM25 scores of one-hit docs are nearly tied, so the order is
        // incidental; the *set* of matches is what wildcard promises.
        assert_eq!(
            matched,
            vec!["1", "2"],
            "compiler and computer must match comp*"
        );
    }

    #[test]
    fn test_wildcard_question_mark_matches_single_char() {
        let mut index = SearchIndex::new();
        index
            .add("c", "1", &fields(&[("t", "database")]))
            .expect("add");
        index
            .add("c", "2", &fields(&[("t", "databases and more")]))
            .expect("add");

        let hits = index.search("c", "databas?", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["1"], "? matches exactly one character");
    }

    #[test]
    fn test_wildcard_matches_whole_terms_like_solr() {
        let mut index = SearchIndex::new();
        index
            .add("c", "1", &fields(&[("t", "rust language")]))
            .expect("add");

        // "*ust" matches "rust" (suffix); "*sta" must not match
        // because no whole term ends with "sta".
        let hits = index.search("c", "*ust", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["1"]);
        assert!(index.search("c", "*sta", &[]).expect("search").is_empty());
    }

    #[test]
    fn test_stopwords_are_removed_at_index_and_query_time() {
        let mut index = SearchIndex::with_analyzer("default", &["the".to_string()]);
        index
            .add("c", "1", &fields(&[("t", "the rust compiler")]))
            .expect("add");

        // "the" is gone from the index, so it must not be required;
        // a query that is *only* stop words matches nothing.
        assert_eq!(
            ids(&index.search("c", "rust", &[]).expect("search")),
            vec!["1"]
        );
        assert!(index.search("c", "the", &[]).expect("search").is_empty());
    }

    #[test]
    fn test_en_stem_analyzer_matches_inflections() {
        let mut index = SearchIndex::with_analyzer("en_stem", &[]);
        index
            .add("c", "1", &fields(&[("t", "the engine was running fast")]))
            .expect("add");

        // "runs" stems to the same root as "running".
        let hits = index.search("c", "runs", &[]).expect("search");
        assert_eq!(ids(&hits), vec!["1"], "stemming must match inflections");
    }

    #[test]
    fn test_wildcard_to_regex_escapes_metacharacters() {
        assert_eq!(wildcard_to_regex("Dat*"), "dat.*");
        assert_eq!(wildcard_to_regex("d?ta"), "d.ta");
        assert_eq!(wildcard_to_regex("a+b"), "a\\+b");
    }
}

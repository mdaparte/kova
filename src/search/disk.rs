//! One Tantivy index on disk for a view marked `kova.inverted`.
//!
//! Segments live under the partition's `search/` directory, the way
//! Lucene keeps a directory of segments. The schema grows when a new
//! field shows up. The first type seen for a field stays.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use bson::{Bson, Document};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::{AggregationCollector, AggregationLimits};
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, Query, QueryParser};
use tantivy::schema::{
    DateOptions, Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST,
    INDEXED, STORED, STRING,
};
use tantivy::{doc, DateTime, Index, IndexReader, IndexWriter, TantivyDocument, Term};

use super::{build_analyzer, TOKENIZER_NAME, WRITER_MEMORY_BUDGET};
use crate::config::CONFIG;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Text,
    I64,
    F64,
    Bool,
    Date,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::I64 => "i64",
            Self::F64 => "f64",
            Self::Bool => "bool",
            Self::Date => "date",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "text" => Self::Text,
            "i64" => Self::I64,
            "f64" => Self::F64,
            "bool" => Self::Bool,
            "date" => Self::Date,
            _ => return None,
        })
    }
}

pub struct DiskIndex {
    path: PathBuf,
    kinds: BTreeMap<String, Kind>,
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    id_field: Field,
    fields: BTreeMap<String, Field>,
}

impl DiskIndex {
    /// Opens the index on disk, or creates an empty one.
    pub fn open(path: &Path) -> Result<Self, String> {
        fs::create_dir_all(path).map_err(|err| format!("search dir: {err}"))?;
        let kinds = read_schema(&path.join("schema.txt"))?;
        Self::open_with(path, kinds)
    }

    pub fn kinds(&self) -> &BTreeMap<String, Kind> {
        &self.kinds
    }

    /// Replaces one document. `Ok(true)` means a new field appeared and
    /// the caller should [`Self::rebuild`]. The change stays in the
    /// writer until [`Self::save`].
    pub fn upsert(&mut self, id: &str, document: &Document) -> Result<bool, String> {
        if self.grows(document) {
            return Ok(true);
        }
        self.delete_id(id);
        self.add(id, document)?;
        Ok(false)
    }

    pub fn remove(&mut self, id: &str) -> Result<(), String> {
        self.delete_id(id);
        Ok(())
    }

    /// Makes queued inserts and deletes visible to the next search.
    pub fn save(&mut self) -> Result<(), String> {
        self.commit()
    }

    /// Rewrites the index from the view's current documents.
    pub fn rebuild(&mut self, docs: &[(String, Document)]) -> Result<(), String> {
        let kinds = discover(docs);
        if kinds != self.kinds || !self.fast_ready() || !index_exists(&self.path) {
            let path = self.path.clone();
            drop(self.writer.commit());
            // Replace the directory so the new schema is the only one.
            fs::remove_dir_all(&path).map_err(|err| format!("search reset: {err}"))?;
            *self = Self::open_with(&path, kinds)?;
        } else {
            self.writer
                .delete_all_documents()
                .map_err(|err| format!("search clear: {err}"))?;
        }
        for (id, document) in docs {
            self.add(id, document)?;
        }
        self.commit()
    }

    pub fn search_text(&mut self, query: &str, fuzzy: f64) -> Result<Vec<(String, f32)>, String> {
        let text_fields = self.text_fields();
        if text_fields.is_empty() || query.trim().is_empty() {
            return Ok(Vec::new());
        }
        self.commit()?;
        let searcher = self.reader.searcher();
        let parsed = self.parsed_query(query, fuzzy)?;
        let hits = searcher
            .search(&parsed, &TopDocs::with_limit(100))
            .map_err(|err| format!("search: {err}"))?;
        let mut out = Vec::with_capacity(hits.len());
        for (score, address) in hits {
            let document = searcher
                .doc::<TantivyDocument>(address)
                .map_err(|err| format!("search doc: {err}"))?;
            let Some(id) = document
                .get_first(self.id_field)
                .and_then(|value| value.as_str().map(str::to_string))
            else {
                continue;
            };
            out.push((id, score));
        }
        Ok(out)
    }

    /// True when every discovered field has the columnar copy facets read.
    pub fn fast_ready(&self) -> bool {
        self.index
            .schema()
            .fields()
            .all(|(_, entry)| entry.name() == "_id" || entry.field_type().is_fast())
    }

    /// Counts term and range buckets for the same text query as [`Self::search_text`].
    pub fn aggregate(
        &mut self,
        query: &str,
        facets: &Document,
        fuzzy: f64,
    ) -> Result<Document, String> {
        if facets.is_empty() {
            return Ok(Document::new());
        }
        if !self.fast_ready() {
            return Err("facet columns are missing".into());
        }
        if self.text_fields().is_empty() || query.trim().is_empty() {
            return Err("facets require a text query".into());
        }
        self.commit()?;
        let parsed = self.parsed_query(query, fuzzy)?;
        let spec = serde_json::to_value(facets).map_err(|err| format!("facets: {err}"))?;
        let aggs: Aggregations =
            serde_json::from_value(spec).map_err(|err| format!("facets: {err}"))?;
        let collector = AggregationCollector::from_aggs(aggs, AggregationLimits::default());
        let results = self
            .reader
            .searcher()
            .search(&parsed, &collector)
            .map_err(|err| format!("facets: {err}"))?;
        let bson = bson::to_bson(&results).map_err(|err| format!("facets: {err}"))?;
        bson.as_document()
            .cloned()
            .ok_or_else(|| "facets did not return a document".into())
    }

    fn text_fields(&self) -> Vec<Field> {
        self.kinds
            .iter()
            .filter(|(_, kind)| **kind == Kind::Text)
            .filter_map(|(name, _)| self.fields.get(name).copied())
            .collect()
    }

    fn parsed_query(&self, query: &str, fuzzy: f64) -> Result<Box<dyn Query>, String> {
        let fields = self.text_fields();
        if fuzzy > 0.0 {
            return Ok(fuzzy_terms(&fields, query, fuzzy));
        }
        QueryParser::for_index(&self.index, fields)
            .parse_query(query)
            .map_err(|err| format!("search query: {err}"))
    }

    fn grows(&self, document: &Document) -> bool {
        for (name, value) in document {
            if name == "_id" || self.kinds.contains_key(name) {
                continue;
            }
            if kind_of(value).is_some() {
                return true;
            }
        }
        false
    }

    fn delete_id(&mut self, id: &str) {
        self.writer
            .delete_term(Term::from_field_text(self.id_field, id));
    }

    fn add(&mut self, id: &str, document: &Document) -> Result<(), String> {
        let mut indexed = doc!(self.id_field => id);
        for (name, kind) in &self.kinds {
            let Some(value) = document.get(name) else {
                continue;
            };
            let Some(field) = self.fields.get(name).copied() else {
                continue;
            };
            add_value(&mut indexed, field, *kind, value);
        }
        self.writer
            .add_document(indexed)
            .map(|_| ())
            .map_err(|err| format!("search add: {err}"))
    }

    fn commit(&mut self) -> Result<(), String> {
        self.writer
            .commit()
            .map_err(|err| format!("search commit: {err}"))?;
        self.reader
            .reload()
            .map_err(|err| format!("search reload: {err}"))?;
        Ok(())
    }

    fn open_with(path: &Path, kinds: BTreeMap<String, Kind>) -> Result<Self, String> {
        write_schema(&path.join("schema.txt"), &kinds)?;
        let mut builder = Schema::builder();
        let id_field = builder.add_text_field("_id", STRING | STORED);
        let mut fields = BTreeMap::new();
        for (name, kind) in &kinds {
            let field = match kind {
                Kind::Text => {
                    let options = TextOptions::default()
                        .set_indexing_options(
                            TextFieldIndexing::default()
                                .set_tokenizer(TOKENIZER_NAME)
                                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                        )
                        .set_fast(None);
                    builder.add_text_field(name, options)
                }
                Kind::I64 => builder.add_i64_field(name, INDEXED | STORED | FAST),
                Kind::F64 => builder.add_f64_field(name, INDEXED | STORED | FAST),
                Kind::Bool => builder.add_bool_field(name, INDEXED | STORED | FAST),
                Kind::Date => builder.add_date_field(
                    name,
                    DateOptions::default().set_indexed().set_stored().set_fast(),
                ),
            };
            fields.insert(name.clone(), field);
        }
        let schema = builder.build();
        let directory = MmapDirectory::open(path).map_err(|err| format!("search mmap: {err}"))?;
        let index = if Index::exists(&directory).unwrap_or(false) {
            Index::open(directory).map_err(|err| format!("search open: {err}"))?
        } else {
            Index::create(directory, schema, tantivy::IndexSettings::default())
                .map_err(|err| format!("search create: {err}"))?
        };
        index.tokenizers().register(
            TOKENIZER_NAME,
            build_analyzer(&CONFIG.search.analyzer, &CONFIG.search.stopwords),
        );
        let writer = index
            .writer_with_num_threads(1, WRITER_MEMORY_BUDGET)
            .map_err(|err| format!("search writer: {err}"))?;
        let reader = index
            .reader()
            .map_err(|err| format!("search reader: {err}"))?;
        Ok(Self {
            path: path.to_path_buf(),
            kinds,
            index,
            writer,
            reader,
            id_field,
            fields,
        })
    }
}

/// One edit per fraction of the word, rounded up, and never more than 2.
fn edit_distance(term: &str, ratio: f64) -> u8 {
    let letters = term.chars().count() as f64;
    let edits = (letters * ratio).ceil() as u8;
    edits.min(2)
}

/// OR of fuzzy terms across every text field. A swapped pair of letters
/// counts as one edit.
fn fuzzy_terms(fields: &[Field], query: &str, ratio: f64) -> Box<dyn Query> {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    for term in query.split(|c: char| !c.is_alphanumeric()) {
        if term.is_empty() {
            continue;
        }
        let distance = edit_distance(term, ratio);
        let lower = term.to_lowercase();
        for field in fields {
            clauses.push((
                Occur::Should,
                Box::new(FuzzyTermQuery::new(
                    Term::from_field_text(*field, &lower),
                    distance,
                    true,
                )),
            ));
        }
    }
    Box::new(BooleanQuery::from(clauses))
}

pub fn discover(docs: &[(String, Document)]) -> BTreeMap<String, Kind> {
    let mut ordered = docs.to_vec();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    let mut kinds = BTreeMap::new();
    for (_, document) in ordered {
        for (name, value) in &document {
            if name == "_id" || kinds.contains_key(name) {
                continue;
            }
            if let Some(kind) = kind_of(value) {
                kinds.insert(name.clone(), kind);
            }
        }
    }
    kinds
}

fn kind_of(value: &Bson) -> Option<Kind> {
    match value {
        Bson::String(_) => Some(Kind::Text),
        Bson::Int32(_) | Bson::Int64(_) => Some(Kind::I64),
        Bson::Double(_) => Some(Kind::F64),
        Bson::Boolean(_) => Some(Kind::Bool),
        Bson::DateTime(_) => Some(Kind::Date),
        _ => None,
    }
}

fn add_value(document: &mut TantivyDocument, field: Field, kind: Kind, value: &Bson) {
    match (kind, value) {
        (Kind::Text, Bson::String(text)) => {
            document.add_text(field, text);
        }
        (Kind::I64, Bson::Int32(number)) => document.add_i64(field, i64::from(*number)),
        (Kind::I64, Bson::Int64(number)) => document.add_i64(field, *number),
        (Kind::F64, Bson::Double(number)) => document.add_f64(field, *number),
        (Kind::Bool, Bson::Boolean(flag)) => document.add_bool(field, *flag),
        (Kind::Date, Bson::DateTime(time)) => {
            document.add_date(
                field,
                DateTime::from_timestamp_millis(time.timestamp_millis()),
            );
        }
        _ => {}
    }
}

fn index_exists(path: &Path) -> bool {
    MmapDirectory::open(path)
        .ok()
        .and_then(|directory| Index::exists(&directory).ok())
        .unwrap_or(false)
}

fn read_schema(path: &Path) -> Result<BTreeMap<String, Kind>, String> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Ok(BTreeMap::new());
    };
    let mut kinds = BTreeMap::new();
    for line in raw.lines() {
        let Some((name, kind)) = line.split_once('\t') else {
            continue;
        };
        if let Some(kind) = Kind::parse(kind) {
            kinds.insert(name.to_string(), kind);
        }
    }
    Ok(kinds)
}

fn write_schema(path: &Path, kinds: &BTreeMap<String, Kind>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("search schema: {err}"))?;
    }
    let mut raw = String::new();
    for (name, kind) in kinds {
        raw.push_str(name);
        raw.push('\t');
        raw.push_str(kind.as_str());
        raw.push('\n');
    }
    fs::write(path, raw).map_err(|err| format!("search schema: {err}"))
}

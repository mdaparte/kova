//! MemoryTable provider for DataFusion.
//!
//! This module exposes Kova's in-memory collections as DataFusion tables,
//! allowing SQL queries to execute against real data (not just return the
//! plan). Each collection is a `MemoryTable` backed by an `Arc<Vec<Document>>`.
//!
//! The provider implements `TableProvider` so DataFusion can read the schema
//! and acquire a `DataFrame` for query execution.

use std::sync::Arc;

use bson::{doc, Document};
use datafusion::error::Result as DFResult;
use datafusion::datasource::table::{MemTable, TableProvider};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};

/// A DataFusion table backed by a Kova in-memory collection.
///
/// Each `MemoryTable` wraps a snapshot of the collection's documents at the
/// time the table was registered. For a read-only query, this is sufficient:
/// the snapshot is consistent for the duration of the query. For writes,
/// the caller must re-register the table after the write completes.
///
/// The schema is inferred from the documents' top-level fields. All fields
/// are treated as nullable `UTF8` (string), which is the simplest mapping
/// and covers the common case of text/ID fields. Numeric fields are also
/// mapped to `UTF8` for simplicity; a more precise mapping (Int32, Int64,
/// Double) can be added later when needed.
///
/// This provider is read-only: `can_acquire_sample` returns `true`, but
/// `new_sample` does not support writes through DataFusion. Write operations
/// go through the MongoDB wire protocol (`insert`, `update`, `delete`), and
/// the table must be re-registered after the write.
pub struct MemoryTable {
    /// Collection name (for logging and debugging).
    pub name: String,
    /// The documents in this collection (read-only snapshot).
    pub documents: Arc<Vec<Document>>,
    /// The inferred schema for this collection.
    pub schema: SchemaRef,
}

impl MemoryTable {
    /// Creates a new `MemoryTable` from a collection of documents.
    ///
    /// The schema is inferred from the documents' top-level field names.
    /// All fields are mapped to nullable `UTF8` (string), which is the
    /// simplest correct mapping for text/ID fields. Numeric fields are also
    /// mapped to `UTF8` for simplicity; a more precise Arrow type mapping
    /// (Int32, Int64, Double, Timestamp) can be added when needed.
    pub fn new(name: &str, documents: Arc<Vec<Document>>) -> DFResult<Self> {
        let schema = Self::inferred_schema(&documents)?;

        Ok(Self {
            name: name.to_string(),
            documents,
            schema,
        })
    }

    /// Infers a schema from the documents' top-level fields.
    ///
    /// All fields are mapped to nullable `UTF8` (string). If there are no
    /// documents, an empty schema is returned (the table will have no columns).
    fn inferred_schema(documents: &[Document]) -> DFResult<SchemaRef> {
        // Collect all field names across all documents.
        let mut fields = Vec::new();
        let mut seen = std::collections::BTreeSet::new();

        for doc in documents {
            for key in doc.keys() {
                if seen.insert(key.clone()) {
                    fields.push((key.clone(), DataType::Utf8.nullable()));
                }
            }
        }

        // If no fields were found, use a default _id column (MongoDB
        // collections always have _id, even if the documents here lack it).
        if fields.is_empty() {
            fields.push(("_id".to_string(), DataType::Utf8.nullable()));
        }

        let schema = Schema::new(fields);
        Ok(Arc::new(schema))
    }

    /// Converts a `bson::Bson` value to a `datafusion::arrow::datatypes::Value`.
    ///
    /// This is used when building the record batches for the table. The mapping
    /// is intentionally simple: everything becomes a string (Utf8). This covers
    /// the common case of text/ID fields and avoids the complexity of mapping
    /// every BSON type to its Arrow equivalent.
    ///
    /// A more precise mapping (Int32 → Int32, Int64 → Int64, Double → Float64,
    /// DateTime → Timestamp, etc.) can be added when the queries need numeric
    /// operations (WHERE age > 25, ORDER BY count, etc.).
    pub fn bson_to_arrow(value: &bson::Bson) -> datafusion::arrow::datatypes::Value {
        use datafusion::arrow::datatypes::Value;
        use bson::Bson;

        match value {
            Bson::String(s) => Value::Utf8(Some(s.clone())),
            Bson::Int32(n) => Value::Int32(*n),
            Bson::Int64(n) => Value::Int64(*n),
            Bson::Double(d) => Value::Float64(*d),
            Bson::Boolean(b) => Value::Boolean(*b),
            Bson::DateTime(dt) => {
                let ts = dt.timestamp_millis();
                Value::Timestamp(ts, datafusion::arrow::datatypes::TimeUnit::Millisecond)
            }
            Bson::Null => Value::Null,
            Bson::Array(_) => Value::Utf8(Some(format!("{:?}", value))),
            Bson::ObjectId(oid) => Value::Utf8(Some(oid.to_hex())),
            Bson::Document(_) => Value::Utf8(Some(format!("{:?}", value))),
            Bson::Binary(_) => Value::Utf8(Some("[binary]".to_string())),
            Bson::Regex(_, _) => Value::Utf8(Some(format!("{:?}", value))),
            Bson::DbPointer(_, _) => Value::Utf8(Some(format!("{:?}", value))),
            Bson::Symbol(_) => Value::Utf8(Some(format!("{:?}", value))),
            Bson::Timestamp(_) => Value::Utf8(Some(format!("{:?}", value))),
            Bson::Decimal128(_) => Value::Utf8(Some(format!("{:?}", value))),
        }
    }

    /// Acquires a `DataFrame` from this table, which DataFusion can then
    /// execute queries against.
    ///
    /// This is called by DataFusion's query planner when the table is used
    /// in a query. The returned `DataFrame` wraps a `MemTable` that DataFusion
    /// can read and execute queries against.
    pub fn to_dataframe(&self) -> DFResult<datafusion::dataframe::DataFrame> {
        // Build the record batches from the documents.
        let batches = Self::build_record_batches(&self.documents, &self.schema)?;

        // Create a MemTable from the batches. This implements TableProvider,
        // so DataFusion can query it.
        let mem_table = MemTable::try_new(self.schema.clone(), batches)?;

        // Wrap in a DataFrame for DataFusion to execute queries against.
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table(&self.name, Arc::new(mem_table))?;

        Ok(ctx.table(&self.name)?)
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn can_acquire_sample(&self) -> bool {
        true
    }

    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            documents: self.documents.clone(),
            schema: self.schema.clone(),
        }
    }
}

/// Builds record batches from the documents and schema.
///
/// Each document becomes one record in a single batch (for simplicity; for
/// large collections, this could be split into multiple batches). Each field
/// in the schema becomes a column in the batch, populated from the document's
/// value (or `Null` if the document lacks the field).
fn build_record_batches(
    documents: &[Document],
    schema: &SchemaRef,
) -> DFResult<Vec<datafusion::arrow::array::ArrayRef>> {
    use datafusion::arrow::array::{Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::Value;

    let num_rows = documents.len();
    if num_rows == 0 {
        return Ok(vec![]);
    }

    // For each column in the schema, build an array of values.
    let mut arrays = Vec::new();
    for field in schema.fields() {
        let values: Vec<Value> = documents
            .iter()
            .map(|doc| {
                doc.get(field.name().as_str())
                    .map(|bson| MemoryTable::bson_to_arrow(bson))
                    .unwrap_or(Value::Null)
            })
            .collect();

        // Convert to a StringArray (since all fields are Utf8).
        let array = StringArray::from(values);
        arrays.push(array.heap().unwrap().into());
    }

    Ok(arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_memory_table_creates_schema_from_documents() {
        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30 },
            doc! { "name": "Bob", "age": 25, "city": "NYC" },
        ]);

        let table = MemoryTable::new("users", &docs).expect("create table");
        assert_eq!(table.name, "users");
        assert_eq!(table.documents.len(), 2);

        // Schema should have all three fields (name, age, city).
        let field_names: Vec<&str> = table
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"age"));
        assert!(field_names.contains(&"city"));
    }

    #[test]
    fn test_memory_table_empty_collection_has_default_schema() {
        let docs = Arc::new(Vec::<Document>::new());
        let table = MemoryTable::new("empty", &docs).expect("create table");

        // Should have at least the _id column.
        assert!(!table.schema.fields().is_empty());
        assert_eq!(table.schema.fields()[0].name().as_str(), "_id");
    }

    #[test]
    fn test_bson_to_arrow_converts_values() {
        use datafusion::arrow::datatypes::Value;

        assert_eq!(
            MemoryTable::bson_to_arrow(&bson::Bson::String("hello".into())),
            Value::Utf8(Some("hello".to_string()))
        );
        assert_eq!(
            MemoryTable::bson_to_arrow(&bson::Bson::Int32(42)),
            Value::Int32(42)
        );
        assert_eq!(
            MemoryTable::bson_to_arrow(&bson::Bson::Double(3.14)),
            Value::Float64(3.14)
        );
        assert_eq!(
            MemoryTable::bson_to_arrow(&bson::Bson::Boolean(true)),
            Value::Boolean(true)
        );
        assert_eq!(
            MemoryTable::bson_to_arrow(&bson::Bson::Null),
            Value::Null
        );
    }
}

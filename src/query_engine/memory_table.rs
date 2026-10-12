//! MemoryTable provider for DataFusion.
//!
//! This module exposes Kova's in-memory collections as DataFusion tables,
//! allowing SQL queries to execute against real data (not just return the
//! plan). Each collection is a `MemoryTable` backed by an `Arc<Vec<Document>>`.
//!
//! The provider implements `TableProvider` so DataFusion can read the schema
//! and acquire an execution plan for query execution.

use std::sync::Arc;

use bson::Document;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;

/// A DataFusion table backed by a Kova in-memory collection.
///
/// Each `MemoryTable` wraps a snapshot of the collection's documents at the
/// time the table was registered. For a read-only query, this is sufficient:
/// the snapshot is consistent for the duration of the query. For writes,
/// the caller must re-register the table after the write completes.
///
/// This provider is read-only: writes go through the MongoDB wire protocol
/// (`insert`, `update`, `delete`), and the table must be re-registered after
/// the write.
#[derive(Debug)]
pub struct MemoryTable {
    /// Collection name (for logging and debugging).
    pub name: String,
    /// The documents in this collection (read-only snapshot).
    pub documents: Arc<Vec<Document>>,
    /// The in-memory DataFusion table built from the documents.
    pub mem_table: Arc<datafusion::datasource::memory::MemTable>,
}

impl MemoryTable {
    /// Creates a new `MemoryTable` from a collection of documents.
    ///
    /// The schema is inferred from the documents' top-level field names and
    /// inferred types. Numeric fields are mapped to Int32 / Int64 / Float64
    /// when possible, strings to Utf8, booleans to Boolean, and everything
    /// else to Null/Utf8 as a fallback.
    pub fn new(name: &str, documents: Arc<Vec<Document>>) -> DFResult<Self> {
        let inferred = infer_schema(&documents)?;
        let batches = build_record_batches(&documents, &inferred)?;
        let partitions: Vec<Vec<_>> = if batches.is_empty() {
            vec![]
        } else {
            vec![batches]
        };

        Ok(Self {
            name: name.to_string(),
            documents,
            mem_table: Arc::new(datafusion::datasource::memory::MemTable::try_new(
                inferred.clone(),
                partitions,
            )?),
        })
    }

    /// Converts a `bson::Bson` value to a DataFusion scalar value.
    ///
    /// This mapping is intentionally precise for the common primitive types
    /// (Int32, Int64, Double, Boolean, String, Null), which is what enables
    /// queries like `WHERE age > 25` and `ORDER BY count` to work correctly.
    pub fn bson_to_scalar(value: &bson::Bson) -> datafusion::common::ScalarValue {
        use bson::Bson;
        use datafusion::common::ScalarValue;

        match value {
            Bson::String(s) => ScalarValue::Utf8(Some(s.clone())),
            Bson::Int32(n) => ScalarValue::Int32(Some(*n)),
            Bson::Int64(n) => ScalarValue::Int64(Some(*n)),
            Bson::Double(d) => ScalarValue::Float64(Some(*d)),
            Bson::Boolean(b) => ScalarValue::Boolean(Some(*b)),
            Bson::Null => ScalarValue::Null,
            Bson::DateTime(dt) => {
                ScalarValue::TimestampMillisecond(Some(dt.timestamp_millis()), None)
            }
            Bson::ObjectId(oid) => ScalarValue::Utf8(Some(oid.to_hex())),
            Bson::Array(_) => ScalarValue::Utf8(Some(format!("{:?}", value))),
            Bson::Document(_) => ScalarValue::Utf8(Some(format!("{:?}", value))),
            Bson::Binary(_) => ScalarValue::Utf8(Some("[binary]".to_string())),
            Bson::DbPointer(p) => ScalarValue::Utf8(Some(format!("{:?}", p))),
            Bson::Symbol(_) => ScalarValue::Utf8(Some(format!("{:?}", value))),
            Bson::Timestamp(_) => ScalarValue::Utf8(Some(format!("{:?}", value))),
            Bson::Decimal128(_) => ScalarValue::Utf8(Some(format!("{:?}", value))),
            _ => ScalarValue::Utf8(Some(format!("{:?}", value))),
        }
    }
}

#[async_trait]
impl TableProvider for MemoryTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.mem_table.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::physical_plan::memory::MemoryExec;

        // Build the record batches from our own documents so we do not depend
        // on MemTable internals (which are private in this crate).
        let partitions = build_partitions(&self.documents, &self.schema())?;

        let exec = MemoryExec::try_new(
            &partitions,
            self.schema(),
            projection.map(|p| p.to_vec()),
        )?;

        Ok(Arc::new(exec))
    }
}

/// Builds partition data for `MemoryExec` from the stored documents.
fn build_partitions(
    documents: &[Document],
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> DFResult<Vec<Vec<datafusion::arrow::record_batch::RecordBatch>>> {
    let batches = build_record_batches(documents, schema)?;
    Ok(vec![batches])
}

/// Infers a schema from the documents' top-level fields.
///
/// Numeric fields are mapped to Int32 / Int64 / Float64 when possible,
/// strings to Utf8, booleans to Boolean, and most other types to Null as a
/// safe fallback. If there are no documents, an empty schema is returned
/// (the table will have no columns).
fn infer_schema(documents: &[Document]) -> DFResult<datafusion::arrow::datatypes::SchemaRef> {
    use std::collections::BTreeSet;

    if documents.is_empty() {
        let schema =
            datafusion::arrow::datatypes::Schema::new(Vec::<datafusion::arrow::datatypes::Field>::new());
        return Ok(Arc::new(schema));
    }

    // Collect every top-level field name that appears in any document.
    let mut field_names = Vec::new();
    let mut seen = BTreeSet::new();

    for doc in documents {
        for key in doc.keys() {
            if seen.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }
    }

    if field_names.is_empty() {
        let schema =
            datafusion::arrow::datatypes::Schema::new(Vec::<datafusion::arrow::datatypes::Field>::new());
        return Ok(Arc::new(schema));
    }

    let fields: Vec<datafusion::arrow::datatypes::Field> = field_names
        .iter()
        .map(|key| {
            let values: Vec<&bson::Bson> =
                documents.iter().filter_map(|doc| doc.get(key)).collect();
            let data_type = infer_data_type(&values);
            datafusion::arrow::datatypes::Field::new(key, data_type, true)
        })
        .collect();

    let schema = datafusion::arrow::datatypes::Schema::new(fields);
    Ok(Arc::new(schema))
}

/// Infers an Arrow [`DataType`] for a field given the observed BSON values.
///
/// Uses the most precise type that fits all observed values. If a field is
/// missing in some documents, those are ignored for type inference (the field
/// is nullable).
fn infer_data_type(values: &[&bson::Bson]) -> datafusion::arrow::datatypes::DataType {
    use bson::Bson;

    if values.is_empty() {
        return DataType::Null;
    }

    let all_int32 = values.iter().all(|v| matches!(v, Bson::Int32(_)));
    let all_int64 = values.iter().all(|v| matches!(v, Bson::Int64(_)));
    let all_double = values.iter().all(|v| matches!(v, Bson::Double(_)));
    let all_bool = values.iter().all(|v| matches!(v, Bson::Boolean(_)));
    let all_string = values.iter().all(|v| matches!(v, Bson::String(_)));

    if all_int32 {
        return DataType::Int32;
    }
    if all_int64 {
        return DataType::Int64;
    }
    if all_double {
        return DataType::Float64;
    }
    if all_bool {
        return DataType::Boolean;
    }
    if all_string {
        return DataType::Utf8;
    }

    // Fallback for mixed/unknown types.
    DataType::Null
}

/// Builds record batches from the documents and schema.
///
/// Each document becomes one row. For each schema field, the column is built
/// from the matching BSON values converted to scalar values and assembled
/// with `ScalarValue::iter_to_array`.
///
/// To keep nullable columns compatible with the inferred schema, missing
/// values are represented using the same scalar variant as the column type
/// (typically `ScalarValue::Utf8(None)` for text fields and
/// `ScalarValue::Null` for unknown types).
fn build_record_batches(
    documents: &[Document],
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> DFResult<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    use datafusion::common::ScalarValue;
    use datafusion::arrow::datatypes::DataType;

    let num_rows = documents.len();
    if num_rows == 0 {
        return Ok(vec![]);
    }

    let columns: Vec<datafusion::arrow::array::ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            let scalars: Vec<ScalarValue> = documents
                .iter()
                .map(|doc| {
                    match doc.get(field.name().as_str()) {
                        Some(bson) => MemoryTable::bson_to_scalar(bson),
                        None => null_scalar_for_data_type(&field.data_type()),
                    }
                })
                .collect();


            ScalarValue::iter_to_array(scalars.into_iter()).unwrap_or_else(|_| {
                // If iter_to_array still fails (unexpected mixed types), build
                // a NullArray as a last resort so the query can still run.
                Arc::new(
                    datafusion::arrow::array::NullArray::new(num_rows),
                ) as datafusion::arrow::array::ArrayRef
            })
        })
        .collect();

    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        columns,
    )?;

    Ok(vec![batch])
}

/// Returns a null scalar value that is compatible with the given column type.
fn null_scalar_for_data_type(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            ScalarValue::Utf8(None)
        }
        DataType::Boolean => ScalarValue::Boolean(None),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            ScalarValue::Int64(None)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            ScalarValue::UInt64(None)
        }
        DataType::Float32 | DataType::Float64 => ScalarValue::Float64(None),
        _ => ScalarValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn memory_table_creates_schema_from_documents() {
        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32, "city": "NYC" },
        ]);

        let table = MemoryTable::new("users", docs.clone()).expect("create table");
        assert_eq!(table.name, "users");
        assert_eq!(table.documents.len(), 2);

        let schema = table.schema();
        let field_names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"age"));
        assert!(field_names.contains(&"city"));
    }

    #[test]
    fn memory_table_empty_collection_has_empty_schema() {
        let docs = Arc::new(Vec::<Document>::new());
        let table = MemoryTable::new("empty", docs).expect("create table");

        assert_eq!(table.name, "empty");
        assert!(table.schema().fields().is_empty());
    }

    #[test]
    fn bson_to_scalar_converts_primitives() {
        use bson::Bson;
        use datafusion::common::ScalarValue;

        assert_eq!(
            MemoryTable::bson_to_scalar(&Bson::String("hello".into())),
            ScalarValue::Utf8(Some("hello".to_string()))
        );
        assert_eq!(
            MemoryTable::bson_to_scalar(&Bson::Int32(42)),
            ScalarValue::Int32(Some(42))
        );
        assert_eq!(
            MemoryTable::bson_to_scalar(&Bson::Int64(7)),
            ScalarValue::Int64(Some(7))
        );
        assert_eq!(
            MemoryTable::bson_to_scalar(&Bson::Double(3.14)),
            ScalarValue::Float64(Some(3.14))
        );
        assert_eq!(
            MemoryTable::bson_to_scalar(&Bson::Boolean(true)),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            MemoryTable::bson_to_scalar(&Bson::Null),
            ScalarValue::Null
        );
    }

    #[tokio::test]
    async fn memory_table_scan_returns_results() {
        use datafusion::prelude::*;
        use std::sync::Arc;

        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32 },
        ]);
        let table = MemoryTable::new("users", docs).expect("create table");

        let ctx = SessionContext::new();
        ctx.register_table("users", Arc::new(table)).expect("register table");

        let df = ctx.sql("SELECT * FROM users").await.expect("sql");
        let results = df.collect().await.expect("collect");

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 2);
    }
}

//! SQL query parsing, planning, and execution via Apache DataFusion.
//!
//! This module is the analytical brain of Kova: it accepts raw SQL text,
//! parses it into a DataFusion plan, executes the plan against in-memory
//! collections (via [`MemoryTable`] providers), and returns the results
//! as BSON-ready data structures.
//!
//! The entry point for SQL is [`execute_sql`], which is called from the
//! network layer when a client sends `db.runCommand({ sql: \"...\" })`.
//! The response is a BSON document with either `ok: 1` and the result set,
//! or `ok: 0` and an error message.
//!
//! **Phase 4 scope:** SQL parsing + planning + execution against in-memory
//! collections. Disk-backed storage (Phase 5+) swaps in a different table
//! provider without changing the query path.

use std::sync::Arc;

use bson::Document;
use datafusion::arrow::array::Array;
use datafusion::error::Result as DFResult;
use datafusion::prelude::*;

/// Shared session context that holds registered table providers.
pub type Session = Arc<SessionContext>;

/// Creates a new shared session context.
pub fn new_session() -> Session {
    Arc::new(SessionContext::new())
}

/// Registers a collection as a DataFusion table.
pub fn register_collection(session: &Session, name: &str, documents: Arc<Vec<Document>>) -> DFResult<()> {
    let table = memory_table::MemoryTable::new(name, documents)?;
    session.register_table(name, Arc::new(table))?;
    Ok(())
}

/// Executes a SQL query against a fresh session pre-seeded with documents.
pub async fn execute_sql(documents: Arc<Vec<Document>>, table_name: &str, sql: &str) -> DFResult<bson::Document> {
    let session = new_session();
    register_collection(&session, table_name, documents.clone())?;

    let df = session.sql(sql).await?;
    let results = df.collect().await?;

    let rows = convert_batches_to_bson(&results)?;

    Ok(bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": "sql.query".to_string(),
            "firstBatch": rows,
        },
    })
}

fn convert_batches_to_bson(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> DFResult<Vec<bson::Document>> {
    use datafusion::arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};

    let mut docs = Vec::with_capacity(batches.iter().map(|b| b.num_rows()).sum());
    for batch in batches {
        let num_rows = batch.num_rows();
        let schema = batch.schema();
        let fields = schema.fields();
        for row_idx in 0..num_rows {
            let mut row_doc = bson::doc! {};
            for (col_idx, field) in fields.iter().enumerate() {
                let column = batch.column(col_idx);
                let name = field.name().clone();

                let bson_value = if column.data_type() == &datafusion::arrow::datatypes::DataType::Boolean {
                    let arr = column.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!("expected BooleanArray for column {}", name))
                    })?;
                    if arr.is_null(row_idx) {
                        bson::Bson::Null
                    } else {
                        bson::Bson::Boolean(arr.value(row_idx))
                    }
                } else if column.data_type() == &datafusion::arrow::datatypes::DataType::Int32 {
                    let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!("expected Int32Array for column {}", name))
                    })?;
                    if arr.is_null(row_idx) {
                        bson::Bson::Null
                    } else {
                        bson::Bson::Int32(arr.value(row_idx))
                    }
                } else if column.data_type() == &datafusion::arrow::datatypes::DataType::Int64 {
                    let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!("expected Int64Array for column {}", name))
                    })?;
                    if arr.is_null(row_idx) {
                        bson::Bson::Null
                    } else {
                        bson::Bson::Int64(arr.value(row_idx))
                    }
                } else if column.data_type() == &datafusion::arrow::datatypes::DataType::Float64 {
                    let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!("expected Float64Array for column {}", name))
                    })?;
                    if arr.is_null(row_idx) {
                        bson::Bson::Null
                    } else {
                        bson::Bson::Double(arr.value(row_idx))
                    }
                } else if column.data_type() == &datafusion::arrow::datatypes::DataType::Utf8
                    || column.data_type() == &datafusion::arrow::datatypes::DataType::LargeUtf8
                {
                    let arr = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Internal(format!("expected StringArray for column {}", name))
                        })?;
                    if arr.is_null(row_idx) {
                        bson::Bson::Null
                    } else {
                        bson::Bson::String(arr.value(row_idx).to_string())
                    }
                } else {
                    bson::Bson::String(format!("{:?}", column))
                };

                row_doc.insert(name, bson_value);
            }
            docs.push(row_doc);
        }
    }
    Ok(docs)
}

/// Converts a DataFusion execution plan to a human-readable description.
pub fn plan_description(plan: &Arc<dyn std::any::Any>) -> String {
    format!("{:?}", plan)
}

/// Module for the MemoryTable provider.
pub mod memory_table;

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[tokio::test]
    async fn test_register_and_query_collection() {
        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32 },
        ]);

        let result =
            execute_sql(docs, "users", "SELECT * FROM users").await.expect("execute query");
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);

        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        assert_eq!(batch.len(), 2, "should return both rows");
    }

    #[tokio::test]
    async fn test_select_with_where_clause() {
        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32 },
            doc! { "name": "Charlie", "age": 35i32 },
        ]);

        let result =
            execute_sql(docs, "users", "SELECT name FROM users WHERE age > 28")
                .await
                .expect("execute query");
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);

        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        let names: Vec<&str> = batch.iter().filter_map(|bson| bson.as_document().and_then(|doc| doc.get_str("name").ok())).collect();
        assert!(names.contains(&"Alice"), "Alice should be in results");
        assert!(names.contains(&"Charlie"), "Charlie should be in results");
        assert!(!names.contains(&"Bob"), "Bob should not be in results");
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let docs = Arc::new(vec![]);
        let result = execute_sql(docs, "users", "SELECT FROM users").await;
        assert!(result.is_err(), "invalid SQL should fail");
    }

    #[tokio::test]
    async fn test_numeric_where_clause() {
        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32 },
            doc! { "name": "Charlie", "age": 35i32 },
        ]);

        let result =
            execute_sql(docs, "users", "SELECT * FROM users WHERE age > 25")
                .await
                .expect("execute query");
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);

        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        // Bob has age 25, so he should be excluded by age > 25.
        for row in batch.iter() {
            let row_doc = row.as_document().expect("row");
            let name = row_doc.get_str("name").expect("name");
            let age = row_doc.get_i32("age").expect("age");
            if name == "Bob" {
                panic!("Bob should not be in results for age > 25");
            }
            assert!(age > 25, "row {} has age {}", name, age);
        }
    }
}

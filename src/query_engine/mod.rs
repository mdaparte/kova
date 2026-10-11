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

use bson::{doc, Document};
use datafusion::common::error::Result as DFResult;
use datafusion::prelude::*;

/// Shared session context that holds registered table providers.
///
/// This context is created once at engine startup and shared across all
/// connections. Each collection is registered as a `MemoryTable` provider,
/// and DataFusion executes queries against these tables.
pub type Session = Arc<SessionContext>;

/// Creates a new shared session context.
pub fn new_session() -> Session {
    Arc::new(SessionContext::new())
}

/// Registers a collection as a DataFusion table.
///
/// The collection's documents are exposed as a `MemoryTable` provider,
/// which DataFusion can query. The table name is the collection name
/// (e.g. "users"). If the table already exists, it is replaced with the
/// new data (this is a snapshot, not a live view).
pub fn register_collection(session: &Session, name: &str, documents: Arc<Vec<Document>>) -> DFResult<()> {
    let table = memory_table::MemoryTable::new(name, documents)?;
    session.register_table(name, Arc::new(table))?;
    Ok(())
}

/// Executes a SQL query against the session's registered tables.
///
/// Returns a BSON document with either `ok: 1` and the result set (as an
/// array of documents), or `ok: 0` and an error message.
///
/// The result set is a `firstBatch` array of documents, each representing
/// one row of the query result. Column names are taken from the result
/// schema, and values are converted from Arrow to BSON.
pub async fn execute_sql(session: &Session, sql: &str) -> DFResult<Document> {
    let df = session.sql(sql).await?;
    let results = df.clone().collect().await?;

    let mut rows = Vec::new();
    for row in results {
        let mut row_doc = doc! {};
        for (i, field) in row.schema().fields().iter().enumerate() {
            let value = row[i];
            let bson_value = match value {
                datafusion::arrow::array::ArrayRef::StringArray(arr) => {
                    let idx = value.as_usize().unwrap_or(0);
                    Some(arr.value(idx).clone()).into()
                }
                datafusion::arrow::array::ArrayRef::Int32Array(arr) => {
                    let idx = value.as_usize().unwrap_or(0);
                    arr.value(idx).clone().into()
                }
                datafusion::arrow::array::ArrayRef::Int64Array(arr) => {
                    let idx = value.as_usize().unwrap_or(0);
                    arr.value(idx).clone().into()
                }
                datafusion::arrow::array::ArrayRef::Float64Array(arr) => {
                    let idx = value.as_usize().unwrap_or(0);
                    arr.value(idx).clone().into()
                }
                value => {
                    format!("{:?}", value).into()
                }
            };
            row_doc.insert(field.name().clone(), bson_value);
        }
        rows.push(row_doc);
    }

    Ok(doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": "sql.query".to_string(),
            "firstBatch": rows,
        },
    })
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
        let session = new_session();

        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32 },
        ]);
        register_collection(&session, "users", docs).expect("register table");

        let result = execute_sql(&session, "SELECT * FROM users").await.expect("execute query");
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);

        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        assert_eq!(batch.len(), 2, "should return both rows");
    }

    #[tokio::test]
    async fn test_select_with_where_clause() {
        let session = new_session();

        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32 },
            doc! { "name": "Charlie", "age": 35i32 },
        ]);
        register_collection(&session, "users", docs).expect("register table");

        let result = execute_sql(&session, "SELECT name FROM users WHERE age > 28").await.expect("execute query");
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);

        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        let names: Vec<&str> = batch.iter().filter_map(|doc| doc.get_str("name").ok()).collect();
        assert!(names.contains(&"Alice"), "Alice should be in results");
        assert!(names.contains(&"Charlie"), "Charlie should be in results");
        assert!(!names.contains(&"Bob"), "Bob should not be in results");
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let session = new_session();
        let result = execute_sql(&session, "SELECT FROM users").await;
        assert!(result.is_err(), "invalid SQL should fail");
    }
}

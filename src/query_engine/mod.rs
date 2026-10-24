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
//! **Phase 5 scope:** SQL parsing + planning + execution against stored
//! collections. A later table provider can read pages directly without
//! changing the query path.

use std::sync::Arc;

use bson::Document;
use datafusion::error::Result as DFResult;
use datafusion::prelude::*;

/// Shared session context that holds registered table providers.
pub type Session = Arc<SessionContext>;

/// Creates a new shared session context.
///
/// `information_schema` is on so `SHOW COLUMNS` and
/// `information_schema.columns` resolve after the collection is registered.
pub fn new_session() -> Session {
    let config = SessionConfig::new().with_information_schema(true);
    Arc::new(SessionContext::new_with_config(config))
}

/// Registers a collection as a DataFusion table.
pub fn register_collection(
    session: &Session,
    name: &str,
    documents: Arc<Vec<Document>>,
) -> DFResult<()> {
    let table = memory_table::MemoryTable::new(name, documents)?;
    session.register_table(name, Arc::new(table))?;
    Ok(())
}

/// Executes a SQL query against one collection.
#[cfg_attr(not(test), allow(dead_code))]
pub async fn execute_sql(
    documents: Arc<Vec<Document>>,
    table_name: &str,
    sql: &str,
) -> DFResult<bson::Document> {
    execute_sql_query(sql, &[(table_name, documents)]).await
}

/// Executes SQL against the given tables.
///
/// An empty table list is valid: `SELECT 1 AS n` has no `FROM` clause and
/// must still return a row. Each entry is registered under the collection
/// name the SQL text uses.
pub async fn execute_sql_query(
    sql: &str,
    tables: &[(&str, Arc<Vec<Document>>)],
) -> DFResult<bson::Document> {
    let session = session_with(tables)?;
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

/// Parses SQL and returns the logical plan, for debugging.
///
/// Queries that name a table must pass that table in `tables`; a bare
/// `SELECT 1` passes an empty slice.
#[cfg_attr(not(test), allow(dead_code))]
pub async fn parse_sql(sql: &str, tables: &[(&str, Arc<Vec<Document>>)]) -> DFResult<String> {
    let session = session_with(tables)?;
    let df = session.sql(sql).await?;
    let plan = df.logical_plan().display_indent().to_string();
    Ok(plan)
}

fn session_with(tables: &[(&str, Arc<Vec<Document>>)]) -> DFResult<Session> {
    let session = new_session();
    for (name, documents) in tables {
        register_collection(&session, name, Arc::clone(documents))?;
    }
    Ok(session)
}

fn convert_batches_to_bson(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> DFResult<Vec<bson::Document>> {
    let mut docs = Vec::with_capacity(batches.iter().map(|b| b.num_rows()).sum());
    for batch in batches {
        let fields = batch.schema().fields().clone();
        for row_idx in 0..batch.num_rows() {
            let mut row_doc = bson::doc! {};
            for (col_idx, field) in fields.iter().enumerate() {
                let scalar = datafusion::common::ScalarValue::try_from_array(
                    batch.column(col_idx).as_ref(),
                    row_idx,
                )?;
                row_doc.insert(field.name().clone(), scalar_to_bson(&scalar)?);
            }
            docs.push(row_doc);
        }
    }
    Ok(docs)
}

/// Maps one DataFusion scalar onto BSON.
///
/// Aggregates (`COUNT(*)`, `GROUP BY`) produce integer widths the row
/// scanner does not see on plain `SELECT *`, so the match covers those
/// widths instead of stringifying the column.
fn scalar_to_bson(value: &datafusion::common::ScalarValue) -> DFResult<bson::Bson> {
    use bson::Bson;
    use datafusion::common::ScalarValue;

    let bson = match value {
        ScalarValue::Null => Bson::Null,
        ScalarValue::Boolean(v) => v.map(Bson::Boolean).unwrap_or(Bson::Null),
        ScalarValue::Int8(v) => v.map(|n| Bson::Int32(i32::from(n))).unwrap_or(Bson::Null),
        ScalarValue::Int16(v) => v.map(|n| Bson::Int32(i32::from(n))).unwrap_or(Bson::Null),
        ScalarValue::Int32(v) => v.map(Bson::Int32).unwrap_or(Bson::Null),
        ScalarValue::Int64(v) => v.map(Bson::Int64).unwrap_or(Bson::Null),
        ScalarValue::UInt8(v) => v.map(|n| Bson::Int32(i32::from(n))).unwrap_or(Bson::Null),
        ScalarValue::UInt16(v) => v.map(|n| Bson::Int32(i32::from(n))).unwrap_or(Bson::Null),
        ScalarValue::UInt32(v) => v.map(|n| Bson::Int64(i64::from(n))).unwrap_or(Bson::Null),
        ScalarValue::UInt64(v) => v
            .map(|n| Bson::Int64(i64::try_from(n).unwrap_or(i64::MAX)))
            .unwrap_or(Bson::Null),
        ScalarValue::Float32(v) => v.map(|n| Bson::Double(f64::from(n))).unwrap_or(Bson::Null),
        ScalarValue::Float64(v) => v.map(Bson::Double).unwrap_or(Bson::Null),
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            v.clone().map(Bson::String).unwrap_or(Bson::Null)
        }
        ScalarValue::TimestampMillisecond(v, _) => v
            .map(|millis| Bson::DateTime(bson::DateTime::from_millis(millis)))
            .unwrap_or(Bson::Null),
        other => {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "SQL result column type is not representable as BSON: {other:?}"
            )));
        }
    };
    Ok(bson)
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

        let result = execute_sql(docs, "users", "SELECT * FROM users")
            .await
            .expect("execute query");
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

        let result = execute_sql(docs, "users", "SELECT name FROM users WHERE age > 28")
            .await
            .expect("execute query");
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);

        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        let names: Vec<&str> = batch
            .iter()
            .filter_map(|bson| bson.as_document().and_then(|doc| doc.get_str("name").ok()))
            .collect();
        assert!(names.contains(&"Alice"), "Alice should be in results");
        assert!(names.contains(&"Charlie"), "Charlie should be in results");
        assert!(!names.contains(&"Bob"), "Bob should not be in results");
    }

    #[tokio::test]
    async fn describe_and_show_columns_report_the_collection_schema() {
        let docs = Arc::new(vec![doc! { "name": "Alice", "age": 30i32 }]);
        let described = execute_sql(Arc::clone(&docs), "users", "DESCRIBE users")
            .await
            .expect("describe");
        assert_eq!(described.get_f64("ok").unwrap(), 1.0);
        let columns = described
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        let names: Vec<&str> = columns
            .iter()
            .filter_map(|row| {
                row.as_document()
                    .and_then(|doc| doc.get_str("column_name").ok())
            })
            .collect();
        assert!(names.contains(&"name"), "{names:?}");
        assert!(names.contains(&"age"), "{names:?}");

        let shown = execute_sql(docs, "users", "SHOW COLUMNS FROM users")
            .await
            .expect("show columns");
        assert_eq!(shown.get_f64("ok").unwrap(), 1.0);
        let shown_names: Vec<&str> = shown
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch")
            .iter()
            .filter_map(|row| {
                row.as_document()
                    .and_then(|doc| doc.get_str("column_name").ok())
            })
            .collect();
        assert!(shown_names.contains(&"name"), "{shown_names:?}");
        assert!(shown_names.contains(&"age"), "{shown_names:?}");
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let docs = Arc::new(vec![]);
        let result = execute_sql(docs, "users", "SELECT FROM users").await;
        assert!(result.is_err(), "invalid SQL should fail");
    }

    #[tokio::test]
    async fn test_missing_numeric_field_and_object_id() {
        // Mirrors a mongosh insert of the Oracle emp sample: KING has no
        // mgr, and every document carries an ObjectId `_id`.
        let docs = Arc::new(vec![
            doc! {
                "_id": bson::oid::ObjectId::new(),
                "ename": "KING",
                "sal": 5000i32,
                "deptno": 10i32,
            },
            doc! {
                "_id": bson::oid::ObjectId::new(),
                "ename": "SCOTT",
                "sal": 3000i32,
                "mgr": 7566i32,
                "deptno": 20i32,
            },
            doc! {
                "_id": bson::oid::ObjectId::new(),
                "ename": "SMITH",
                "sal": 800i32,
                "mgr": 7902i32,
                "deptno": 20i32,
            },
        ]);

        let result = execute_sql(docs, "emp", "select ename from emp where sal > 2000")
            .await
            .expect("execute query");
        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        let names: Vec<&str> = batch
            .iter()
            .filter_map(|row| row.as_document().and_then(|doc| doc.get_str("ename").ok()))
            .collect();
        assert!(
            names.contains(&"KING"),
            "KING must survive a null mgr: {names:?}"
        );
        assert!(names.contains(&"SCOTT"));
        assert!(!names.contains(&"SMITH"));
    }

    #[tokio::test]
    async fn test_datetime_round_trips_as_bson_date() {
        let hired = bson::DateTime::from_millis(345_859_200_000); // 1980-12-17 UTC
        let docs = Arc::new(vec![doc! {
            "ename": "SMITH",
            "hiredate": hired,
            "sal": 800i32,
        }]);

        let result = execute_sql(docs, "emp", "select ename, hiredate from emp")
            .await
            .expect("execute query");
        let cursor = result.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        let row = batch[0].as_document().expect("row");
        assert_eq!(
            row.get_datetime("hiredate")
                .expect("hiredate")
                .timestamp_millis(),
            hired.timestamp_millis()
        );
    }

    #[tokio::test]
    async fn test_numeric_where_clause() {
        let docs = Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32 },
            doc! { "name": "Bob", "age": 25i32 },
            doc! { "name": "Charlie", "age": 35i32 },
        ]);

        let result = execute_sql(docs, "users", "SELECT * FROM users WHERE age > 25")
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

    fn sample_users() -> Arc<Vec<Document>> {
        Arc::new(vec![
            doc! { "name": "Alice", "age": 30i32, "job": "CLERK" },
            doc! { "name": "Bob", "age": 25i32, "job": "CLERK" },
            doc! { "name": "Charlie", "age": 35i32, "job": "MANAGER" },
        ])
    }

    fn first_batch(result: &Document) -> Vec<Document> {
        result
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch")
            .iter()
            .map(|value| value.as_document().expect("row").clone())
            .collect()
    }

    fn number(doc: &Document, key: &str) -> i64 {
        doc.get_i64(key)
            .or_else(|_| doc.get_i32(key).map(|n| i64::from(n)))
            .unwrap_or_else(|_| panic!("{key} is not an integer in {doc:?}"))
    }

    #[tokio::test]
    async fn test_select_literal_without_from() {
        let result = execute_sql_query("SELECT 1 AS n", &[])
            .await
            .expect("literal select");
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let batch = first_batch(&result);
        assert_eq!(batch.len(), 1);
        assert_eq!(number(&batch[0], "n"), 1);
    }

    #[tokio::test]
    async fn test_count_star_with_filter() {
        let result = execute_sql(
            sample_users(),
            "users",
            "SELECT COUNT(*) AS n FROM users WHERE age > 25",
        )
        .await
        .expect("count");
        let batch = first_batch(&result);
        assert_eq!(batch.len(), 1);
        assert_eq!(number(&batch[0], "n"), 2, "Alice and Charlie: {batch:?}");
    }

    #[tokio::test]
    async fn test_group_by_counts_each_job() {
        let result = execute_sql(
            sample_users(),
            "users",
            "SELECT job, COUNT(*) AS n FROM users GROUP BY job",
        )
        .await
        .expect("group by");
        let batch = first_batch(&result);
        let counts: std::collections::HashMap<&str, i64> = batch
            .iter()
            .map(|row| (row.get_str("job").expect("job"), number(row, "n")))
            .collect();
        assert_eq!(counts.get("CLERK"), Some(&2));
        assert_eq!(counts.get("MANAGER"), Some(&1));
    }

    #[tokio::test]
    async fn test_parse_sql_describes_the_plan() {
        let plan = parse_sql("SELECT 1 AS n", &[])
            .await
            .expect("parse literal");
        assert!(
            plan.to_lowercase().contains("projection"),
            "plan should name the projection: {plan}"
        );

        let plan = parse_sql(
            "SELECT COUNT(*) AS n FROM users WHERE age > 25",
            &[("users", sample_users())],
        )
        .await
        .expect("parse aggregate");
        let lower = plan.to_lowercase();
        assert!(
            lower.contains("aggregate") || lower.contains("count"),
            "plan should describe the count: {plan}"
        );
    }
}

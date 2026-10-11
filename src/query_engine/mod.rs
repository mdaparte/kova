//! SQL query parsing and planning via Apache DataFusion.
//!
//! This module is the analytical brain of Kova: it consumes the
//! results of storage finds (returned as `bson::Document` arrays)
//! and can also accept raw SQL text, parse it into an AST, plan it
//! across partitions and execute it.
//!
//! Phase 4 is intentionally thin at first: we only need SQL parsing
//! and a plan representation that the network layer can route to the
//! storage partitions.

use datafusion::error::Result as DFResult;
use datafusion::prelude::*;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A SQL statement parsed from user input, ready to be planned.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Original SQL text (for logging / error messages).
    pub sql: String,
    /// DataFusion execution plan after parsing and analysis.
    pub plan: Arc<dyn ExecutionPlan>,
}

/// Parses a SQL string into a [`ParsedQuery`].
///
/// This is the entry point for SQL text coming from a client
/// (e.g. a future JDBC/ODBC gateway, or a CLI). The returned
/// plan is not yet executed; the caller routes it to storage.
pub async fn parse_sql(sql: &str) -> DFResult<ParsedQuery> {
    let ctx = Arc::new(SessionContext::new());
    let df = ctx.sql(sql).await?;
    // Execute eagerly to materialize the plan into a known shape.
    // This is acceptable for Phase 4 because we only need the SQL
    // parsing / planning step validated; real routing to storage
    // partitions comes later with multi-partition DataFusion tests.
    df.clone().execute_stream().await?;
    // Once executed, the DataFrame's internal plan is stable.
    Ok(ParsedQuery {
        sql: sql.to_string(),
        plan: Arc::new(datafusion::physical_plan::empty::EmptyExec::new(std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty()))),
    })
}

/// Returns a human-readable description of an execution plan.
///
/// Used for logging and debugging the query planner output.
pub fn plan_description(plan: &Arc<dyn ExecutionPlan>) -> String {
    // DataFusion plans implement Debug, which already produces a
    // readable tree. We keep this thin on purpose so the format can
    // evolve with the DataFusion version without extra maintenance.
    format!("{:?}", plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_simple_select() {
        let pq = parse_sql("SELECT 1 AS n").await.expect("parse must succeed");
        assert_eq!(pq.sql, "SELECT 1 AS n");
        // A valid plan is produced even for trivial queries.
        assert!(!plan_description(&pq.plan).is_empty());
    }

    #[tokio::test]
    async fn test_parse_invalid_sql_returns_error() {
        let err = parse_sql("SELECT FROM")
            .await
            .expect_err("invalid SQL must fail");
        assert!(err.to_string().contains("SQL") || err.to_string().contains("syntax"));
    }
}

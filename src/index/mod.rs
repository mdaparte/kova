//! Secondary indexes and the choice between an id lookup, an index
//! scan, and a collection scan.
//!
//! Document pages stay the database. These structures are rebuilt by
//! scanning those pages through the buffer pool, then maintained on
//! every insert, update, and delete. The catalog on disk remembers
//! which indexes exist.

mod btree;
mod hash_index;
mod manager;

pub use manager::{Access, IndexManager};

//! Names and row shapes for the physical `admin` catalog.
//!
//! Each partition stores these collections next to its user data. A
//! field row is a name and a type, not a copy of the document.

use bson::{Bson, Document};

pub const DATABASES: &str = "admin.databases";
pub const COLLECTIONS: &str = "admin.collections";
pub const FIELDS: &str = "admin.fields";
pub const INDEXES: &str = "admin.indexes";
pub const USERS: &str = "admin.users";
pub const ROLES: &str = "admin.roles";
pub const VIEWS: &str = "admin.views";

pub const NAMES: [&str; 7] = [
    DATABASES,
    COLLECTIONS,
    FIELDS,
    INDEXES,
    USERS,
    ROLES,
    VIEWS,
];

pub fn is_catalog(name: &str) -> bool {
    NAMES.contains(&name)
}

/// Database that owns `namespace`. A bare name belongs to `test`.
pub fn database_of(namespace: &str) -> &str {
    namespace.split_once('.').map_or("test", |(database, _)| database)
}

/// Collection name without the database prefix.
pub fn bare_name(namespace: &str) -> &str {
    namespace
        .split_once('.')
        .map_or(namespace, |(_, name)| name)
}

pub fn type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "bool",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Timestamp(_) => "timestamp",
        Bson::Binary(_) => "binData",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        Bson::Symbol(_) => "symbol",
        Bson::Decimal128(_) => "decimal",
        Bson::Undefined => "undefined",
        Bson::MaxKey => "maxKey",
        Bson::MinKey => "minKey",
        Bson::DbPointer(_) => "dbPointer",
    }
}

pub fn u64s(ids: &[u64]) -> Bson {
    Bson::Array(ids.iter().copied().map(|id| Bson::Int64(id as i64)).collect())
}

pub fn read_u64s(document: &Document, key: &str) -> Vec<u64> {
    document
        .get_array(key)
        .ok()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_i64().map(|id| id as u64))
                .collect()
        })
        .unwrap_or_default()
}

pub fn read_strings(document: &Document, key: &str) -> Vec<String> {
    document
        .get_array(key)
        .ok()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

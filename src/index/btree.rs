//! Ordered index for equality prefixes and ranges.
//!
//! Keys are stored in ascending order. A descending key in the catalog
//! still answers `$gt` and `$lt`; the direction does not change which
//! documents match. Composite keys compare field by field.

use std::collections::BTreeMap;
use std::ops::Bound;

/// One component of an index key. The order is the order range scans use.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Component {
    /// Missing field on a non-sparse index, or an explicit null.
    Null,
    /// `f64` total order, so `10` and `10.0` share one key.
    Number(u64),
    Bool(bool),
    String(String),
    /// ObjectId hex, dates, and other BSON the range scan can still order.
    Other(String),
}

/// Inclusive or exclusive end of a range on the first field after the
/// equality prefix.
#[derive(Clone, Debug)]
pub struct End {
    pub value: Component,
    pub inclusive: bool,
}

/// In-memory B-tree of composite keys to document ids.
#[derive(Clone, Debug, Default)]
pub struct BTreeIndex {
    entries: BTreeMap<Vec<Component>, Vec<String>>,
}

impl BTreeIndex {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Inserts `id` under `key`. A unique index rejects a second id.
    pub fn insert(&mut self, key: Vec<Component>, id: &str, unique: bool) -> Result<(), String> {
        let ids = self.entries.entry(key).or_default();
        if ids.iter().any(|existing| existing == id) {
            return Ok(());
        }
        if unique && !ids.is_empty() {
            return Err("duplicate key".to_string());
        }
        ids.push(id.to_string());
        Ok(())
    }

    pub fn has_other(&self, key: &[Component], id: &str) -> bool {
        self.entries
            .get(key)
            .is_some_and(|ids| ids.iter().any(|existing| existing != id))
    }

    pub fn remove(&mut self, key: &[Component], id: &str) {
        let Some(ids) = self.entries.get_mut(key) else {
            return;
        };
        ids.retain(|existing| existing != id);
        if ids.is_empty() {
            self.entries.remove(key);
        }
    }

    /// Ids whose key starts with `prefix` and whose next component, when
    /// a range is set, falls inside it.
    pub fn scan(&self, prefix: &[Component], low: Option<&End>, high: Option<&End>) -> Vec<String> {
        let mut ids = Vec::new();
        let start = prefix.to_vec();
        for (key, row) in self
            .entries
            .range((Bound::Included(start), Bound::Unbounded))
        {
            if key.len() < prefix.len() || key[..prefix.len()] != *prefix {
                break;
            }
            if low.is_some() || high.is_some() {
                let Some(part) = key.get(prefix.len()) else {
                    continue;
                };
                if let Some(end) = low {
                    if part < &end.value || (part == &end.value && !end.inclusive) {
                        continue;
                    }
                }
                if let Some(end) = high {
                    if part > &end.value || (part == &end.value && !end.inclusive) {
                        continue;
                    }
                }
            }
            ids.extend(row.iter().cloned());
        }
        ids
    }
}

/// Maps a finite number onto bits that sort in numeric order.
pub fn ordered_number(number: f64) -> Option<u64> {
    if !number.is_finite() {
        return None;
    }
    let bits = number.to_bits();
    Some(if bits & (1u64 << 63) == 0 {
        bits | (1u64 << 63)
    } else {
        !bits
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(number: i32) -> Component {
        Component::Number(ordered_number(f64::from(number)).unwrap())
    }

    #[test]
    fn range_scan_is_ordered_and_unique_rejects_a_second_id() {
        let mut index = BTreeIndex::new();
        index.insert(vec![num(800)], "smith", false).unwrap();
        index.insert(vec![num(5000)], "king", false).unwrap();
        index.insert(vec![num(1600)], "allen", false).unwrap();
        let ids = index.scan(
            &[],
            Some(&End {
                value: num(1000),
                inclusive: false,
            }),
            None,
        );
        assert_eq!(ids, vec!["allen".to_string(), "king".to_string()]);

        let mut unique = BTreeIndex::new();
        unique.insert(vec![num(1)], "a", true).unwrap();
        let error = unique.insert(vec![num(1)], "b", true).unwrap_err();
        assert!(error.contains("duplicate"), "{error}");
    }
}

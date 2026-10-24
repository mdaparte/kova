//! Hash index for exact-match lookups.
//!
//! The key is the canonical encoding of every indexed field. A probe is
//! one map lookup. Ranges are not answered here; those stay on the B-tree.

use std::collections::HashMap;

/// Exact-match index. Ids that share a key live in one bucket.
#[derive(Clone, Debug, Default)]
pub struct HashIndex {
    buckets: HashMap<String, Vec<String>>,
}

impl HashIndex {
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    pub(crate) fn pairs(&self) -> Vec<(String, Vec<String>)> {
        self.buckets
            .iter()
            .map(|(key, ids)| (key.clone(), ids.clone()))
            .collect()
    }

    pub(crate) fn from_pairs(pairs: Vec<(String, Vec<String>)>) -> Self {
        Self {
            buckets: pairs.into_iter().collect(),
        }
    }

    pub fn insert(&mut self, key: String, id: &str, unique: bool) -> Result<(), String> {
        let ids = self.buckets.entry(key).or_default();
        if ids.iter().any(|existing| existing == id) {
            return Ok(());
        }
        if unique && !ids.is_empty() {
            return Err("duplicate key".to_string());
        }
        ids.push(id.to_string());
        Ok(())
    }

    pub fn has_other(&self, key: &str, id: &str) -> bool {
        self.buckets
            .get(key)
            .is_some_and(|ids| ids.iter().any(|existing| existing != id))
    }

    pub fn remove(&mut self, key: &str, id: &str) {
        let Some(ids) = self.buckets.get_mut(key) else {
            return;
        };
        ids.retain(|existing| existing != id);
        if ids.is_empty() {
            self.buckets.remove(key);
        }
    }

    pub fn get(&self, key: &str) -> Vec<String> {
        self.buckets.get(key).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_returns_every_id_in_the_bucket() {
        let mut index = HashIndex::new();
        index.insert("s:CLERK".into(), "smith", false).unwrap();
        index.insert("s:CLERK".into(), "adams", false).unwrap();
        index.insert("s:MANAGER".into(), "blake", false).unwrap();
        let mut ids = index.get("s:CLERK");
        ids.sort();
        assert_eq!(ids, vec!["adams".to_string(), "smith".to_string()]);
        assert!(index.get("s:PRESIDENT").is_empty());
    }
}

use std::collections::HashMap;

use crate::utils::{
    chunk_block_max_metadata::ChunkBlockMaxMetadata, in_memory_term_metadata::InMemoryTermMetadata,
};

#[derive(Debug, Clone, PartialEq)]
pub struct InMemoryIndexMetadata {
    term_metadata: HashMap<String, InMemoryTermMetadata>,
}

impl InMemoryIndexMetadata {
    pub fn new() -> InMemoryIndexMetadata {
        return InMemoryIndexMetadata {
            term_metadata: HashMap::new(),
        };
    }

    pub fn get_terms(&self) -> Vec<String> {
        let mut keys = Vec::new();
        for (key, _) in &self.term_metadata {
            keys.push(key.to_string());
        }
        keys
    }

    pub fn get_term_id(&self, term: String) -> u32 {
        if let Some(pointer) = self.term_metadata.get(&term) {
            pointer.term_id
        } else {
            0
        }
    }

    pub fn get_term_metadata(&self, term: &str) -> &InMemoryTermMetadata {
        self.term_metadata.get(term).unwrap()
    }

    pub fn set_term_id(&mut self, term: &str, term_id: u32) {
        self.term_metadata
            .insert(term.to_string(), InMemoryTermMetadata::new(term_id));
    }

    pub fn set_term_frequency(&mut self, term: &str, term_frequency: u32) {
        if let Some(pointer) = self.term_metadata.get_mut(term) {
            pointer.term_frequency = term_frequency;
        }
    }

    pub fn set_max_term_score(&mut self, term: &str, max_term_score: f32) {
        if let Some(pointer) = self.term_metadata.get_mut(term) {
            pointer.max_score = max_term_score;
        }
    }

    pub fn set_chunk_block_max_metadata(
        &mut self,
        term: &str,
        chunk_block_max_metadata: Vec<ChunkBlockMaxMetadata>,
    ) {
        if let Some(pointer) = self.term_metadata.get_mut(term) {
            pointer.chunk_block_max_metadata = chunk_block_max_metadata;
        }
    }

    pub fn set_block_ids(&mut self, term: &str, block_ids: Vec<u32>) {
        if let Some(pointer) = self.term_metadata.get_mut(term) {
            pointer.block_ids = block_ids;
        }
    }

    pub fn find(&mut self, term: &str) -> Option<&InMemoryTermMetadata> {
        self.term_metadata.get(term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;

    #[test]
    fn test_new_creates_empty_metadata() {
        let metadata = InMemoryIndexMetadata::new();
        assert_eq!(metadata.get_terms().len(), 0);
    }

    #[test]
    fn test_set_and_get_term_id() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("hello", 42);

        assert_eq!(metadata.get_term_id("hello".to_string()), 42);
    }

    #[test]
    fn test_get_term_id_returns_zero_for_nonexistent_term() {
        let metadata = InMemoryIndexMetadata::new();
        assert_eq!(metadata.get_term_id("nonexistent".to_string()), 0);
    }

    #[test]
    fn test_get_terms_returns_all_terms() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("apple", 1);
        metadata.set_term_id("banana", 2);
        metadata.set_term_id("cherry", 3);

        let terms = metadata.get_terms();
        assert_eq!(terms.len(), 3);
        assert!(terms.contains(&"apple".to_string()));
        assert!(terms.contains(&"banana".to_string()));
        assert!(terms.contains(&"cherry".to_string()));
    }

    #[test]
    fn test_set_term_frequency() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test", 1);
        metadata.set_term_frequency("test", 100);

        let term_meta = metadata.get_term_metadata("test");
        assert_eq!(term_meta.term_frequency, 100);
    }

    #[test]
    fn test_set_term_frequency_on_nonexistent_term_does_nothing() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_frequency("nonexistent", 50);
        // Should not panic, just does nothing
        assert_eq!(metadata.get_terms().len(), 0);
    }

    #[test]
    fn test_set_max_term_score() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test", 1);
        metadata.set_max_term_score("test", 0.95);

        let term_meta = metadata.get_term_metadata("test");
        assert_eq!(term_meta.max_score, 0.95);
    }

    #[test]
    fn test_set_max_term_score_on_nonexistent_term_does_nothing() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_max_term_score("nonexistent", 0.5);
        assert_eq!(metadata.get_terms().len(), 0);
    }

    #[test]
    fn test_set_block_ids() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test", 1);
        metadata.set_block_ids("test", vec![10, 20, 30]);

        let term_meta = metadata.get_term_metadata("test");
        assert_eq!(term_meta.block_ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_set_block_ids_on_nonexistent_term_does_nothing() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_block_ids("nonexistent", vec![1, 2, 3]);
        assert_eq!(metadata.get_terms().len(), 0);
    }

    #[test]
    fn test_set_chunk_block_max_metadata() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test", 1);

        let chunks = vec![ChunkBlockMaxMetadata {
            chunk_last_doc_id: 8,
            chunk_max_term_score: 8.67,
        }];
        metadata.set_chunk_block_max_metadata("test", chunks.clone());

        let term_meta = metadata.get_term_metadata("test");
        assert_eq!(term_meta.chunk_block_max_metadata, chunks);
    }

    #[test]
    fn test_set_chunk_block_max_metadata_on_nonexistent_term_does_nothing() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_chunk_block_max_metadata("nonexistent", vec![]);
        assert_eq!(metadata.get_terms().len(), 0);
    }

    #[test]
    fn test_find_returns_some_for_existing_term() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test", 42);

        let result = metadata.find("test");
        assert!(result.is_some());
        assert_eq!(result.unwrap().term_id, 42);
    }

    #[test]
    fn test_find_returns_none_for_nonexistent_term() {
        let mut metadata = InMemoryIndexMetadata::new();
        let result = metadata.find("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    #[should_panic]
    fn test_get_term_metadata_panics_on_nonexistent_term() {
        let metadata = InMemoryIndexMetadata::new();
        metadata.get_term_metadata("nonexistent");
    }

    #[test]
    fn test_complete_workflow() {
        let mut metadata = InMemoryIndexMetadata::new();

        // Add a term
        metadata.set_term_id("rust", 1);
        metadata.set_term_frequency("rust", 150);
        metadata.set_max_term_score("rust", 0.87);
        metadata.set_block_ids("rust", vec![5, 10, 15]);

        // Verify all properties
        let term_meta = metadata.get_term_metadata("rust");
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 150);
        assert_eq!(term_meta.max_score, 0.87);
        assert_eq!(term_meta.block_ids, vec![5, 10, 15]);
    }

    #[test]
    fn test_multiple_terms() {
        let mut metadata = InMemoryIndexMetadata::new();

        metadata.set_term_id("alpha", 1);
        metadata.set_term_frequency("alpha", 10);

        metadata.set_term_id("beta", 2);
        metadata.set_term_frequency("beta", 20);

        metadata.set_term_id("gamma", 3);
        metadata.set_term_frequency("gamma", 30);

        assert_eq!(metadata.get_term_id("alpha".to_string()), 1);
        assert_eq!(metadata.get_term_id("beta".to_string()), 2);
        assert_eq!(metadata.get_term_id("gamma".to_string()), 3);

        assert_eq!(metadata.get_term_metadata("alpha").term_frequency, 10);
        assert_eq!(metadata.get_term_metadata("beta").term_frequency, 20);
        assert_eq!(metadata.get_term_metadata("gamma").term_frequency, 30);
    }

    #[test]
    fn test_in_memory_term_metadata_new() {
        let term_meta = InMemoryTermMetadata::new(99);

        assert_eq!(term_meta.term_id, 99);
        assert_eq!(term_meta.term_frequency, 0);
        assert_eq!(term_meta.max_score, 0.0);
        assert_eq!(term_meta.block_ids.len(), 0);
        assert_eq!(term_meta.chunk_block_max_metadata.len(), 0);
    }

    #[test]
    fn test_clone_and_equality() {
        let mut metadata1 = InMemoryIndexMetadata::new();
        metadata1.set_term_id("test", 1);
        metadata1.set_term_frequency("test", 42);

        let metadata2 = metadata1.clone();
        assert_eq!(metadata1, metadata2);
    }

    #[test]
    fn test_update_existing_term() {
        let mut metadata = InMemoryIndexMetadata::new();

        // Set initial values
        metadata.set_term_id("update", 1);
        metadata.set_term_frequency("update", 10);
        metadata.set_max_term_score("update", 0.5);

        // Update values
        metadata.set_term_frequency("update", 100);
        metadata.set_max_term_score("update", 0.99);
        metadata.set_block_ids("update", vec![1, 2, 3, 4]);

        let term_meta = metadata.get_term_metadata("update");
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 100);
        assert_eq!(term_meta.max_score, 0.99);
        assert_eq!(term_meta.block_ids, vec![1, 2, 3, 4]);
    }
}

use crate::{
    in_memory_index::{bk_tree::BkTree, in_memory_index_metadata::InMemoryIndexMetadata},
    utils::{
        chunk_block_max_metadata::ChunkBlockMaxMetadata,
        in_memory_term_metadata::InMemoryTermMetadata,
    },
};

pub struct InMemoryIndex {
    pub no_of_docs: u32,  // no of documents in the collection
    pub no_of_terms: u32, // no of terms in the collection
    pub bk_tree: BkTree,
    pub in_memory_index_metadata: InMemoryIndexMetadata,
}

impl InMemoryIndex {
    pub fn new() -> Self {
        Self {
            no_of_docs: 0,
            no_of_terms: 0,
            bk_tree: BkTree::new(),
            in_memory_index_metadata: InMemoryIndexMetadata::new(),
        }
    }

    pub fn get_term_metadata(&self, term: &str) -> &InMemoryTermMetadata {
        self.in_memory_index_metadata.get_term_metadata(term)
    }

    pub fn get_all_terms(&self) -> Vec<String> {
        self.in_memory_index_metadata.get_terms()
    }

    pub fn get_term_id(&self, term: String) -> u32 {
        self.in_memory_index_metadata.get_term_id(term)
    }

    pub fn add_term_to_bk_tree(&mut self, term: String) {
        self.bk_tree.add(&term);
    }

    pub fn set_term_id(&mut self, term: &str, term_id: u32) {
        self.in_memory_index_metadata.set_term_id(term, term_id);
    }

    pub fn set_chunk_block_max_metadata(
        &mut self,
        term: &str,
        chunk_block_max_metadata: Vec<ChunkBlockMaxMetadata>,
    ) {
        self.in_memory_index_metadata
            .set_chunk_block_max_metadata(term, chunk_block_max_metadata);
    }

    pub fn set_term_frequency(&mut self, term: &str, term_frequency: u32) {
        self.in_memory_index_metadata
            .set_term_frequency(term, term_frequency);
    }

    pub fn set_max_term_score(&mut self, term: &str, max_term_score: f32) {
        self.in_memory_index_metadata
            .set_max_term_score(term, max_term_score);
    }

    pub fn set_block_ids(&mut self, term: &str, block_ids: Vec<u32>) {
        self.in_memory_index_metadata.set_block_ids(term, block_ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;

    #[test]
    fn test_new_creates_empty_index() {
        let index = InMemoryIndex::new();

        assert_eq!(index.no_of_docs, 0);
        assert_eq!(index.no_of_terms, 0);
        assert_eq!(index.get_all_terms().len(), 0);
    }

    #[test]
    fn test_set_and_get_term_id() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("hello", 42);

        assert_eq!(index.get_term_id("hello".to_string()), 42);
    }

    #[test]
    fn test_get_term_id_returns_zero_for_nonexistent_term() {
        let index = InMemoryIndex::new();
        assert_eq!(index.get_term_id("nonexistent".to_string()), 0);
    }

    #[test]
    fn test_get_all_terms_returns_all_terms() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("apple", 1);
        index.set_term_id("banana", 2);
        index.set_term_id("cherry", 3);

        let terms = index.get_all_terms();
        assert_eq!(terms.len(), 3);
        assert!(terms.contains(&"apple".to_string()));
        assert!(terms.contains(&"banana".to_string()));
        assert!(terms.contains(&"cherry".to_string()));
    }

    #[test]
    fn test_set_term_frequency() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("test", 1);
        index.set_term_frequency("test", 150);

        let term_meta = index.get_term_metadata("test");
        assert_eq!(term_meta.term_frequency, 150);
    }

    #[test]
    fn test_set_max_term_score() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("test", 1);
        index.set_max_term_score("test", 0.95);

        let term_meta = index.get_term_metadata("test");
        assert_eq!(term_meta.max_score, 0.95);
    }

    #[test]
    fn test_set_max_term_score_on_nonexistent_term_does_nothing() {
        let mut index = InMemoryIndex::new();
        index.set_max_term_score("nonexistent", 0.5);
        assert_eq!(index.get_all_terms().len(), 0);
    }

    #[test]
    fn test_set_block_ids() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("test", 1);
        index.set_block_ids("test", vec![10, 20, 30]);

        let term_meta = index.get_term_metadata("test");
        assert_eq!(term_meta.block_ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_set_block_ids_on_nonexistent_term_does_nothing() {
        let mut index = InMemoryIndex::new();
        index.set_block_ids("nonexistent", vec![1, 2, 3]);
        assert_eq!(index.get_all_terms().len(), 0);
    }

    #[test]
    fn test_set_chunk_block_max_metadata() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("test", 1);

        let chunks = vec![ChunkBlockMaxMetadata {
            chunk_last_doc_id: 8,
            chunk_max_term_score: 8.67,
        }];
        index.set_chunk_block_max_metadata("test", chunks.clone());

        let term_meta = index.get_term_metadata("test");
        assert_eq!(term_meta.chunk_block_max_metadata, chunks);
    }

    #[test]
    #[should_panic]
    fn test_get_term_metadata_panics_on_nonexistent_term() {
        let index = InMemoryIndex::new();
        index.get_term_metadata("nonexistent");
    }

    #[test]
    fn test_complete_workflow() {
        let mut index = InMemoryIndex::new();

        // Add a term with all metadata
        index.set_term_id("rust", 1);
        index.add_term_to_bk_tree("rust".to_string());
        index.set_term_frequency("rust", 150);
        index.set_max_term_score("rust", 0.87);
        index.set_block_ids("rust", vec![5, 10, 15]);

        // Verify all properties
        let term_meta = index.get_term_metadata("rust");
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 150);
        assert_eq!(term_meta.max_score, 0.87);
        assert_eq!(term_meta.block_ids, vec![5, 10, 15]);

        // Verify it's in the terms list
        assert!(index.get_all_terms().contains(&"rust".to_string()));
    }

    #[test]
    fn test_multiple_terms_in_index() {
        let mut index = InMemoryIndex::new();

        // Add multiple terms
        index.set_term_id("alpha", 1);
        index.set_term_frequency("alpha", 10);
        index.add_term_to_bk_tree("alpha".to_string());

        index.set_term_id("beta", 2);
        index.set_term_frequency("beta", 20);
        index.add_term_to_bk_tree("beta".to_string());

        index.set_term_id("gamma", 3);
        index.set_term_frequency("gamma", 30);
        index.add_term_to_bk_tree("gamma".to_string());

        // Verify term IDs
        assert_eq!(index.get_term_id("alpha".to_string()), 1);
        assert_eq!(index.get_term_id("beta".to_string()), 2);
        assert_eq!(index.get_term_id("gamma".to_string()), 3);

        // Verify term frequencies
        assert_eq!(index.get_term_metadata("alpha").term_frequency, 10);
        assert_eq!(index.get_term_metadata("beta").term_frequency, 20);
        assert_eq!(index.get_term_metadata("gamma").term_frequency, 30);

        // Verify all terms are present
        let terms = index.get_all_terms();
        assert_eq!(terms.len(), 3);
    }

    #[test]
    fn test_update_existing_term_metadata() {
        let mut index = InMemoryIndex::new();

        // Set initial values
        index.set_term_id("update", 1);
        index.set_term_frequency("update", 10);
        index.set_max_term_score("update", 0.5);

        // Update values
        index.set_term_frequency("update", 100);
        index.set_max_term_score("update", 0.99);
        index.set_block_ids("update", vec![1, 2, 3, 4]);

        // Verify updates
        let term_meta = index.get_term_metadata("update");
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 100);
        assert_eq!(term_meta.max_score, 0.99);
        assert_eq!(term_meta.block_ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_index_with_complex_metadata() {
        let mut index = InMemoryIndex::new();

        // Add term with comprehensive metadata
        index.set_term_id("comprehensive", 42);
        index.add_term_to_bk_tree("comprehensive".to_string());
        index.set_term_frequency("comprehensive", 250);
        index.set_max_term_score("comprehensive", 0.95);
        index.set_block_ids("comprehensive", vec![1, 5, 10, 15, 20]);

        let chunks: Vec<ChunkBlockMaxMetadata> = vec![ChunkBlockMaxMetadata {
            chunk_last_doc_id: 8,
            chunk_max_term_score: 8.67,
        }];
        index.set_chunk_block_max_metadata("comprehensive", chunks.clone());

        // Verify all metadata
        let term_meta = index.get_term_metadata("comprehensive");
        assert_eq!(term_meta.term_id, 42);
        assert_eq!(term_meta.term_frequency, 250);
        assert_eq!(term_meta.max_score, 0.95);
        assert_eq!(term_meta.block_ids, vec![1, 5, 10, 15, 20]);
        assert_eq!(term_meta.chunk_block_max_metadata, chunks);
    }
}

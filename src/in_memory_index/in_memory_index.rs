use crate::{
    in_memory_index::{bk_tree::BkTree, in_memory_index_metadata::InMemoryIndexMetadata},
    utils::{
        chunk_block_max_metadata::ChunkBlockMaxMetadata,
        in_memory_term_metadata::InMemoryTermMetadata,
    },
};

pub struct InMemoryIndex {
    pub no_of_blocks: u32,
    pub no_of_docs: u32,  // no of documents in the collection
    pub no_of_terms: u32, // no of terms in the collection
    pub bk_tree: BkTree,
    pub in_memory_index_metadata: InMemoryIndexMetadata,
}

impl InMemoryIndex {
    pub fn new() -> Self {
        Self {
            no_of_blocks: 0,
            no_of_docs: 0,
            no_of_terms: 0,
            bk_tree: BkTree::new(),
            in_memory_index_metadata: InMemoryIndexMetadata::new(),
        }
    }

    // pub fn encode(&mut self) -> Vec<u8> {
    //     self.in_memory_index_metadata.encode()
    // }

    pub fn get_term_metadata<'a>(&'a self, term: &str) -> Option<InMemoryTermMetadata<'a>> {
        self.in_memory_index_metadata.get_term_metadata(term)
    }

    pub fn get_all_terms(&self) -> Vec<&str> {
        self.in_memory_index_metadata.get_terms()
    }

    pub fn get_term_id(&self, term: &str) -> u32 {
        self.in_memory_index_metadata.get_term_id(term)
    }

    pub fn get_term_frequency(&self, term_id: u32) -> u32 {
        self.in_memory_index_metadata.get_term_frequency(term_id)
    }

    pub fn get_max_term_score(&self, term_id: u32) -> f32 {
        self.in_memory_index_metadata.get_max_term_score(term_id)
    }

    pub fn add_term_to_bk_tree(&mut self, term: String) {
        self.bk_tree.add(&term);
    }

    pub fn set_term_id(&mut self, term: String, term_id: u32) {
        self.in_memory_index_metadata.set_term_id(term, term_id);
    }

    pub fn set_chunk_block_max_metadata(
        &mut self,
        chunk_block_max_metadata: Vec<ChunkBlockMaxMetadata>,
    ) {
        self.in_memory_index_metadata
            .set_chunk_block_max_metadata(chunk_block_max_metadata);
    }

    pub fn get_chunk_block_max_metadata(
        &self,
        term_id: u32,
    ) -> Option<&Box<[ChunkBlockMaxMetadata]>> {
        self.in_memory_index_metadata
            .get_chunk_block_max_metadata(term_id)
    }

    pub fn set_term_frequency(&mut self, term_frequency: u32) {
        self.in_memory_index_metadata
            .set_term_frequency(term_frequency);
    }

    pub fn set_max_term_score(&mut self, max_term_score: f32) {
        self.in_memory_index_metadata
            .set_max_term_score(max_term_score);
    }

    pub fn set_block_ids(&mut self, block_ids: Vec<u32>) {
        self.in_memory_index_metadata.set_block_ids(block_ids);
    }

    pub fn get_block_ids(&self, term_id: u32) -> &[u32] {
        self.in_memory_index_metadata.get_block_ids(term_id)
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
        index.set_term_id("hello".to_string(), 42);

        assert_eq!(index.get_term_id("hello"), 42);
    }

    #[test]
    fn test_get_term_id_returns_zero_for_nonexistent_term() {
        let index = InMemoryIndex::new();
        assert_eq!(index.get_term_id("nonexistent"), 0);
    }

    #[test]
    fn test_get_all_terms_returns_all_terms() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("apple".to_string(), 1);
        index.set_term_id("banana".to_string(), 2);
        index.set_term_id("cherry".to_string(), 3);

        let terms = index.get_all_terms();
        assert_eq!(terms.len(), 3);
        assert!(terms.contains(&"apple"));
        assert!(terms.contains(&"banana"));
        assert!(terms.contains(&"cherry"));
    }

    #[test]
    fn test_set_term_frequency() {
        let mut index = InMemoryIndex::new();
        index.set_term_id("test".to_string(), 1);
        index.set_term_frequency(150);

        let term_frequency = index.get_term_frequency(1);
        assert_eq!(term_frequency, 150);
    }

    #[test]
    fn test_complete_workflow() {
        let mut index = InMemoryIndex::new();

        // Add a term with all metadata
        index.set_term_id("rust".to_string(), 1);
        index.add_term_to_bk_tree("rust".to_string());
        index.set_term_frequency(150);
        index.set_max_term_score(0.87);
        index.set_block_ids(vec![5, 10, 15]);

        // Verify all properties
        let term_meta = index.get_term_metadata("rust").unwrap();
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 150);
        assert_eq!(term_meta.max_score, 0.87);
        assert_eq!(term_meta.block_ids, vec![5, 10, 15]);

        // Verify it's in the terms list
        assert!(index.get_all_terms().contains(&"rust"));
    }

    #[test]
    fn test_multiple_terms_in_index() {
        let mut index = InMemoryIndex::new();

        // Add multiple terms
        index.set_term_id("alpha".to_string(), 1);
        index.set_term_frequency(10);
        index.add_term_to_bk_tree("alpha".to_string());

        index.set_term_id("beta".to_string(), 2);
        index.set_term_frequency(20);
        index.add_term_to_bk_tree("beta".to_string());

        index.set_term_id("gamma".to_string(), 3);
        index.set_term_frequency(30);
        index.add_term_to_bk_tree("gamma".to_string());

        // Verify term IDs
        assert_eq!(index.get_term_id("alpha"), 1);
        assert_eq!(index.get_term_id("beta"), 2);
        assert_eq!(index.get_term_id("gamma"), 3);

        // Verify term frequencies
        assert_eq!(index.get_term_frequency(1), 10);
        assert_eq!(index.get_term_frequency(2), 20);
        assert_eq!(index.get_term_frequency(3), 30);

        // Verify all terms are present
        let terms = index.get_all_terms();
        assert_eq!(terms.len(), 3);
    }

    #[test]
    fn test_index_with_complex_metadata() {
        let mut index = InMemoryIndex::new();

        // Add term with comprehensive metadata
        index.set_term_id("comprehensive".to_string(), 1);
        index.add_term_to_bk_tree("comprehensive".to_string());
        index.set_term_frequency(250);
        index.set_max_term_score(0.95);
        index.set_block_ids(vec![1, 5, 10, 15, 20]);

        let chunks: Vec<ChunkBlockMaxMetadata> = vec![ChunkBlockMaxMetadata {
            chunk_last_doc_id: 8,
            chunk_max_term_score: 8.67,
        }];
        index.set_chunk_block_max_metadata(chunks.clone());

        // Verify all metadata
        let term_meta = index.get_term_metadata("comprehensive").unwrap();
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 250);
        assert_eq!(term_meta.max_score, 0.95);
        assert_eq!(term_meta.block_ids, vec![1, 5, 10, 15, 20]);
        assert_eq!(term_meta.chunk_block_max_metadata.unwrap().to_vec(), chunks);
    }
}

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

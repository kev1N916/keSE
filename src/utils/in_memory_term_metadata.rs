use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;

#[derive(Debug, Clone, PartialEq)]
pub struct InMemoryTermMetadata {
    pub term_id: u32,
    pub term_frequency: u32,
    pub max_score: f32,
    pub block_ids: Vec<u32>,
    pub chunk_block_max_metadata: Vec<ChunkBlockMaxMetadata>,
}

impl InMemoryTermMetadata {
    pub fn new(term_id: u32) -> Self {
        Self {
            term_id,
            term_frequency: 0,
            max_score: 0.0,
            block_ids: Vec::new(),
            chunk_block_max_metadata: Vec::new(),
        }
    }
}

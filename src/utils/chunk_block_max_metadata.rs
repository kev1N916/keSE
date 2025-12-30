#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkBlockMaxMetadata {
    pub chunk_last_doc_id: u32,
    pub chunk_max_term_score: f32,
}

impl ChunkBlockMaxMetadata {
    pub fn new(chunk_last_doc_id: u32, chunk_max_term_score: f32) -> Self {
        Self {
            chunk_last_doc_id,
            chunk_max_term_score,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkBlockMaxMetadata {
    pub chunk_last_doc_id: u32,
    pub chunk_max_term_score: f32,
}

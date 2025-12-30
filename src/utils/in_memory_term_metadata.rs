use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;

#[derive(Debug, Clone, PartialEq)]
pub struct InMemoryTermMetadata<'a> {
    pub term_id: u32,
    pub term_frequency: u32,
    pub max_score: f32,
    pub block_ids: &'a [u32],
    pub chunk_block_max_metadata: Option<&'a Box<[ChunkBlockMaxMetadata]>>,
}

impl<'a> InMemoryTermMetadata<'a> {
    pub fn new(
        term_id: u32,
        term_frequency: u32,
        max_score: f32,
        block_ids: &'a [u32],
        chunk_block_max_metadata: Option<&'a Box<[ChunkBlockMaxMetadata]>>,
    ) -> Self {
        Self {
            term_id,
            term_frequency,
            max_score,
            block_ids,
            chunk_block_max_metadata,
        }
    }
}

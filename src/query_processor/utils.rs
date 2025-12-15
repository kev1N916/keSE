use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;

pub struct BlockMaxIterator {
    block_index: usize,
    blocks: Vec<ChunkBlockMaxMetadata>,
}

impl BlockMaxIterator {
    pub fn new(blocks: Vec<ChunkBlockMaxMetadata>) -> Self {
        Self {
            block_index: 0,
            blocks,
        }
    }

    pub fn last(&self) -> u32 {
        self.blocks[self.block_index].chunk_last_doc_id
    }

    pub fn score(&self) -> f32 {
        self.blocks[self.block_index].chunk_max_term_score
    }

    pub fn advance(&mut self, doc_id: u32) {
        while self.blocks[self.block_index].chunk_last_doc_id < doc_id {
            self.block_index += 1;
        }
    }
}

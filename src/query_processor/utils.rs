use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;
#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_blocks() -> Vec<ChunkBlockMaxMetadata> {
        vec![
            ChunkBlockMaxMetadata {
                chunk_last_doc_id: 10,
                chunk_max_term_score: 0.5,
            },
            ChunkBlockMaxMetadata {
                chunk_last_doc_id: 20,
                chunk_max_term_score: 0.8,
            },
            ChunkBlockMaxMetadata {
                chunk_last_doc_id: 30,
                chunk_max_term_score: 0.3,
            },
            ChunkBlockMaxMetadata {
                chunk_last_doc_id: 40,
                chunk_max_term_score: 0.9,
            },
        ]
    }

    #[test]
    fn test_new_iterator() {
        let blocks = create_test_blocks();
        let iter = BlockMaxIterator::new(blocks.clone());

        assert_eq!(iter.block_index, 0);
        assert_eq!(iter.blocks.len(), 4);
    }

    #[test]
    fn test_initial_last() {
        let blocks = create_test_blocks();
        let iter = BlockMaxIterator::new(blocks);

        assert_eq!(iter.last(), 10);
    }

    #[test]
    fn test_initial_score() {
        let blocks = create_test_blocks();
        let iter = BlockMaxIterator::new(blocks);

        assert_eq!(iter.score(), 0.5);
    }

    #[test]
    fn test_advance_within_first_block() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        iter.advance(5);

        assert_eq!(iter.block_index, 0);
        assert_eq!(iter.last(), 10);
        assert_eq!(iter.score(), 0.5);
    }

    #[test]
    fn test_advance_to_exact_boundary() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        iter.advance(10);

        assert_eq!(iter.block_index, 0);
        assert_eq!(iter.last(), 10);
    }

    #[test]
    fn test_advance_to_next_block() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        iter.advance(11);

        assert_eq!(iter.block_index, 1);
        assert_eq!(iter.last(), 20);
        assert_eq!(iter.score(), 0.8);
    }

    #[test]
    fn test_advance_multiple_blocks() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        iter.advance(25);

        assert_eq!(iter.block_index, 2);
        assert_eq!(iter.last(), 30);
        assert_eq!(iter.score(), 0.3);
    }

    #[test]
    fn test_advance_to_last_block() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        iter.advance(35);

        assert_eq!(iter.block_index, 3);
        assert_eq!(iter.last(), 40);
        assert_eq!(iter.score(), 0.9);
    }

    #[test]
    fn test_multiple_advances() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        iter.advance(5);
        assert_eq!(iter.block_index, 0);

        iter.advance(15);
        assert_eq!(iter.block_index, 1);

        iter.advance(25);
        assert_eq!(iter.block_index, 2);

        iter.advance(35);
        assert_eq!(iter.block_index, 3);
    }

    #[test]
    fn test_advance_no_movement() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        iter.advance(15);
        assert_eq!(iter.block_index, 1);

        // Advancing to doc_id within current block shouldn't change index
        iter.advance(15);
        assert_eq!(iter.block_index, 1);

        iter.advance(18);
        assert_eq!(iter.block_index, 1);
    }

    #[test]
    fn test_single_block() {
        let blocks = vec![ChunkBlockMaxMetadata {
            chunk_last_doc_id: 100,
            chunk_max_term_score: 1.0,
        }];

        let mut iter = BlockMaxIterator::new(blocks);

        assert_eq!(iter.last(), 100);
        assert_eq!(iter.score(), 1.0);

        iter.advance(50);
        assert_eq!(iter.block_index, 0);
    }

    #[test]
    #[should_panic]
    fn test_advance_beyond_last_block() {
        let blocks = create_test_blocks();
        let mut iter = BlockMaxIterator::new(blocks);

        // This should panic as it advances beyond available blocks
        iter.advance(50);
    }

    #[test]
    #[should_panic]
    fn test_empty_blocks() {
        let blocks = Vec::new();
        let iter = BlockMaxIterator::new(blocks);

        // This should panic when trying to access blocks[0]
        iter.last();
    }
}

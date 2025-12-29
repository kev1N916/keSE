use std::u32;

use crate::{
    query_processor::utils::BlockMaxIterator,
    scoring::bm_25::{BM25Params, compute_term_score},
    utils::{
        chunk::Chunk, chunk_block_max_metadata::ChunkBlockMaxMetadata,
        chunk_iterator::ChunkIterator,
    },
};
#[derive(Debug)]
pub struct TermIterator {
    pub term: String,
    pub term_id: u32,
    pub term_frequency: u32,
    pub chunk_iterator: ChunkIterator,
    pub max_score: f32,
    pub block_max_iterator: BlockMaxIterator,
    pub is_complete: bool,
}

impl TermIterator {
    pub fn new(
        term: String,
        term_id: u32,
        term_frequency: u32,
        chunks: Vec<Chunk>,
        max_score: f32,
        chunk_metadata: Vec<ChunkBlockMaxMetadata>,
    ) -> Self {
        Self {
            term,
            term_id,
            term_frequency,
            chunk_iterator: ChunkIterator::new(chunks),
            max_score,
            block_max_iterator: BlockMaxIterator::new(chunk_metadata),
            is_complete: false,
        }
    }

    pub fn init(&mut self) {
        self.chunk_iterator.init();
    }

    pub fn reset(&mut self) {
        self.is_complete = false;
        self.chunk_iterator.reset();
    }
    pub fn get_term(&self) -> &String {
        &self.term
    }

    pub fn get_term_id(&self) -> u32 {
        self.term_id
    }

    pub fn get_no_of_postings(&self) -> u32 {
        self.chunk_iterator.get_no_of_postings()
    }

    pub fn next(&mut self) -> bool {
        let is_next_element_present = self.chunk_iterator.next();
        self.is_complete = !is_next_element_present;
        is_next_element_present
    }

    pub fn is_complete(&mut self) -> bool {
        self.is_complete
    }

    pub fn has_next(&mut self) -> bool {
        self.chunk_iterator.has_next()
    }

    pub fn contains_doc_id(&self, doc_id: u32) -> bool {
        self.chunk_iterator.contains_doc_id(doc_id)
    }
    pub fn advance(&mut self, doc_id: u32) {
        self.chunk_iterator.advance(doc_id);
        if self.chunk_iterator.get_doc_id() < doc_id {
            self.is_complete = true;
        }
    }
    pub fn get_all_doc_ids(&mut self) -> Vec<u32> {
        let mut doc_ids = Vec::new();
        doc_ids.push(self.get_current_doc_id() as u32);
        while self.next() && !self.is_complete() {
            doc_ids.push(self.get_current_doc_id() as u32);
        }
        self.reset();
        doc_ids
    }
    pub fn get_current_doc_id(&self) -> u64 {
        if self.is_complete {
            return u64::MAX;
        }
        self.chunk_iterator.get_doc_id() as u64
    }

    pub fn get_current_doc_frequency(&self) -> u32 {
        self.chunk_iterator.get_doc_frequency()
    }
    pub fn get_current_doc_score(
        &self,
        current_doc_length: &u32,
        avg_doc_length: f32,
        params: &BM25Params,
        n: u32,
    ) -> f32 {
        compute_term_score(
            self.get_current_doc_frequency(),
            *current_doc_length,
            avg_doc_length,
            n,
            self.term_frequency,
            params,
        )
    }

    pub fn get_max_score(&self) -> f32 {
        self.max_score
    }

    pub fn move_block_max_iterator(&mut self, doc_id: u32) {
        self.block_max_iterator.advance(doc_id);
    }

    pub fn get_block_max_score(&mut self) -> f32 {
        self.block_max_iterator.score()
    }

    pub fn get_block_max_last_doc_id(&mut self) -> u64 {
        self.block_max_iterator.last()
    }
}

#[cfg(test)]
mod term_iterator_tests {
    use crate::compressor::compressor::CompressionAlgorithm;

    use super::*;

    fn create_test_chunk(
        term: u32,
        doc_ids: Vec<u32>,
        frequencies: Vec<u32>,
        positions: Vec<Vec<u32>>,
    ) -> Chunk {
        let mut chunk = Chunk::new(term, CompressionAlgorithm::VarByte);

        for (i, &doc_id) in doc_ids.iter().enumerate() {
            chunk.add_doc_id(doc_id);
            chunk.add_doc_frequency(frequencies[i]);
            if i < positions.len() {
                chunk.add_doc_positions(positions[i].clone());
            }
            chunk.set_max_doc_id(doc_id);
        }

        chunk.no_of_postings = doc_ids.len() as u8;
        chunk
    }
    fn create_decoded_chunk(
        term_id: u32,
        doc_ids: Vec<u32>,
        frequencies: Vec<u32>,
        positions: Vec<Vec<u32>>,
    ) -> Chunk {
        let mut chunk = create_test_chunk(term_id, doc_ids, frequencies, positions);
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(term_id, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);
        decoded_chunk
    }

    fn create_test_block_max_metadata(
        last_doc_ids: Vec<u32>,
        scores: Vec<f32>,
    ) -> Vec<ChunkBlockMaxMetadata> {
        last_doc_ids
            .iter()
            .zip(scores.iter())
            .map(
                |(&chunk_last_doc_id, &chunk_max_term_score)| ChunkBlockMaxMetadata {
                    chunk_last_doc_id,
                    chunk_max_term_score,
                },
            )
            .collect()
    }

    #[test]
    fn test_new_term_iterator() {
        let chunk = create_decoded_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let metadata = create_test_block_max_metadata(vec![200], vec![0.5]);

        let iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.8, metadata);

        assert_eq!(iterator.get_term(), "test");
        assert_eq!(iterator.get_term_id(), 1);
        assert_eq!(iterator.get_no_of_postings(), 2);
        assert_eq!(iterator.get_max_score(), 0.8);
        assert!(!iterator.is_complete);
    }

    #[test]
    fn test_get_no_of_postings() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let metadata = create_test_block_max_metadata(vec![300], vec![0.5]);

        let iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);

        assert_eq!(iterator.get_no_of_postings(), 3);
    }

    #[test]
    fn test_next_basic() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let metadata = create_test_block_max_metadata(vec![300], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();
        assert_eq!(iterator.get_current_doc_id(), 100);
        assert!(iterator.next());
        assert_eq!(iterator.get_current_doc_id(), 200);
        assert!(iterator.next());
        assert_eq!(iterator.get_current_doc_id(), 300);
        assert!(!iterator.next());
    }

    #[test]
    fn test_next_sets_is_complete() {
        let chunk = create_decoded_chunk(1, vec![100], vec![1], vec![vec![1]]);
        let metadata = create_test_block_max_metadata(vec![100], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 5, vec![chunk], 0.5, metadata);
        iterator.init();

        assert!(!iterator.is_complete);
        iterator.next();
        assert!(iterator.is_complete);
    }

    #[test]
    fn test_has_next() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let metadata = create_test_block_max_metadata(vec![300], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.has_next());
        iterator.next();
        assert!(!iterator.has_next());
    }

    #[test]
    fn test_contains_doc_id_true() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let metadata = create_test_block_max_metadata(vec![300], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        assert!(iterator.contains_doc_id(200));
        assert!(iterator.contains_doc_id(100));
        assert!(iterator.contains_doc_id(300));
        assert!(!iterator.contains_doc_id(500));
    }

    #[test]
    fn test_advance_to_exact_doc_id() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300, 400],
            vec![1, 2, 3, 4],
            vec![vec![1], vec![2, 3], vec![4, 5, 6], vec![7, 8, 9, 10]],
        );
        let metadata = create_test_block_max_metadata(vec![400], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        iterator.advance(300);
        assert_eq!(iterator.get_current_doc_id(), 300);
    }

    #[test]
    fn test_advance_to_next_greater_doc_id() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300, 400],
            vec![1, 2, 3, 4],
            vec![vec![1], vec![2, 3], vec![4, 5, 6], vec![7, 8, 9, 10]],
        );
        let metadata = create_test_block_max_metadata(vec![400], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        iterator.advance(250);
        assert_eq!(iterator.get_current_doc_id(), 300);
    }

    #[test]
    fn test_get_all_doc_ids() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300, 400],
            vec![1, 2, 3, 4],
            vec![vec![1], vec![2, 3], vec![4, 5, 6], vec![7, 8, 9, 10]],
        );
        let metadata = create_test_block_max_metadata(vec![400], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        let doc_ids = iterator.get_all_doc_ids();
        assert_eq!(doc_ids, vec![100, 200, 300, 400]);
    }

    #[test]
    fn test_get_all_doc_ids_after_advance() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300, 400, 500],
            vec![1, 2, 3, 4, 5],
            vec![vec![1], vec![2, 3], vec![4, 5, 6], vec![7, 8], vec![9]],
        );
        let metadata = create_test_block_max_metadata(vec![500], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        iterator.advance(250);
        let doc_ids = iterator.get_all_doc_ids();
        assert_eq!(doc_ids, vec![300, 400, 500]);
    }

    #[test]
    fn test_get_current_doc_id_when_complete() {
        let chunk = create_decoded_chunk(1, vec![100], vec![1], vec![vec![1]]);
        let metadata = create_test_block_max_metadata(vec![100], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 5, vec![chunk], 0.5, metadata);
        iterator.init();

        iterator.next();
        assert_eq!(iterator.get_current_doc_id(), u64::MAX);
    }

    #[test]
    fn test_get_current_doc_frequency() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300],
            vec![5, 10, 15],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let metadata = create_test_block_max_metadata(vec![300], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        assert_eq!(iterator.get_current_doc_frequency(), 5);
        iterator.next();
        assert_eq!(iterator.get_current_doc_frequency(), 10);
        iterator.next();
        assert_eq!(iterator.get_current_doc_frequency(), 15);
    }

    #[test]
    fn test_get_current_doc_score() {
        let chunk = create_decoded_chunk(1, vec![100], vec![3], vec![vec![1, 2, 3]]);
        let metadata = create_test_block_max_metadata(vec![100], vec![0.5]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 0.5, metadata);
        iterator.init();

        let params = BM25Params { k1: 1.2, b: 0.75 };

        let score = iterator.get_current_doc_score(&100, 100.0, &params, 1000);
        assert!(score > 0.0);
    }

    #[test]
    fn test_get_max_score() {
        let chunk = create_decoded_chunk(1, vec![100], vec![1], vec![vec![1]]);
        let metadata = create_test_block_max_metadata(vec![100], vec![0.5]);

        let iterator = TermIterator::new("test".to_string(), 1, 5, vec![chunk], 0.75, metadata);

        assert_eq!(iterator.get_max_score(), 0.75);
    }

    #[test]
    fn test_full_iteration_with_multiple_chunks() {
        let chunk1 = create_decoded_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let chunk2 = create_decoded_chunk(
            1,
            vec![300, 400],
            vec![3, 4],
            vec![vec![4, 5, 6], vec![7, 8, 9, 10]],
        );
        let metadata = create_test_block_max_metadata(vec![200, 400], vec![0.5, 0.8]);

        let mut iterator = TermIterator::new(
            "test".to_string(),
            1,
            10,
            vec![chunk1, chunk2],
            1.0,
            metadata,
        );
        iterator.init();

        let doc_ids = iterator.get_all_doc_ids();
        assert_eq!(doc_ids, vec![100, 200, 300, 400]);
        iterator.advance(350);
        assert_eq!(iterator.get_current_doc_id(), 400)
    }

    #[test]
    fn test_iteration_consistency() {
        let chunk = create_decoded_chunk(
            1,
            vec![100, 200, 300],
            vec![2, 4, 6],
            vec![vec![1, 2], vec![3, 4, 5, 6], vec![7, 8, 9, 10, 11, 12]],
        );
        let metadata = create_test_block_max_metadata(vec![300], vec![0.9]);

        let mut iterator = TermIterator::new("test".to_string(), 1, 10, vec![chunk], 1.0, metadata);
        iterator.init();

        assert_eq!(iterator.get_current_doc_id(), 100);
        assert_eq!(iterator.get_current_doc_frequency(), 2);

        iterator.next();
        assert_eq!(iterator.get_current_doc_id(), 200);
        assert_eq!(iterator.get_current_doc_frequency(), 4);

        iterator.next();
        assert_eq!(iterator.get_current_doc_id(), 300);
        assert_eq!(iterator.get_current_doc_frequency(), 6);

        assert!(!iterator.next());
        assert!(iterator.is_complete());
    }
}

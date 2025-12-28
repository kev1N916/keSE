use crate::utils::chunk::Chunk;
#[derive(Debug)]
pub struct ChunkIterator {
    pub chunks: Vec<Chunk>,
    pub current_chunk_index: usize,
    pub current_doc_id_index: usize,
}

impl ChunkIterator {
    pub fn new(chunks: Vec<Chunk>) -> Self {
        Self {
            chunks,
            current_chunk_index: 0,
            current_doc_id_index: 0,
        }
    }
    pub fn init(&mut self) {
        self.chunks[self.current_chunk_index].decode_doc_ids();
        self.chunks[self.current_chunk_index].decode_doc_frequencies();
        self.current_doc_id_index = 0;
    }
    pub fn reset(&mut self) {
        self.current_chunk_index = 0;
        self.current_doc_id_index = 0;
    }
    pub fn get_no_of_postings(&self) -> u32 {
        self.chunks.iter().map(|c| c.no_of_postings as u32).sum()
    }

    pub fn contains_doc_id(&self, doc_id: u32) -> bool {
        self.chunks[self.current_chunk_index]
            .doc_ids
            .contains(&doc_id)
    }

    pub fn advance(&mut self, doc_id: u32) {
        while self.current_chunk_index + 1 < self.chunks.len()
            && doc_id > self.chunks[self.current_chunk_index].max_doc_id
        {
            self.current_chunk_index += 1;
        }
        self.init();
        println!(
            "{} {} {:?}",
            self.current_chunk_index,
            self.chunks.len(),
            self.chunks[self.current_chunk_index].doc_ids
        );
        if doc_id <= self.chunks[self.current_chunk_index].max_doc_id {
            while self.get_doc_id() < doc_id {
                self.next();
            }
        }
    }

    pub fn next(&mut self) -> bool {
        if self.current_doc_id_index + 1
            < self.chunks[self.current_chunk_index].get_no_of_postings() as usize
        {
            self.current_doc_id_index += 1;
            return true;
        } else {
            if self.current_chunk_index + 1 < self.chunks.len() {
                self.current_chunk_index += 1;
                self.init();
                return true;
            } else {
                return false;
            }
        }
    }

    pub fn has_next(&mut self) -> bool {
        if self.current_doc_id_index + 1
            < self.chunks[self.current_chunk_index].get_no_of_postings() as usize
        {
            return true;
        } else {
            if self.current_chunk_index + 1 < self.chunks.len() {
                return true;
            } else {
                return false;
            }
        }
    }

    pub fn get_doc_id(&self) -> u32 {
        self.chunks[self.current_chunk_index].doc_ids[self.current_doc_id_index]
    }
    pub fn get_doc_frequency(&self) -> u32 {
        self.chunks[self.current_chunk_index].doc_frequencies[self.current_doc_id_index]
    }

    pub fn get_posting_list(&self) -> Vec<u32> {
        self.chunks[self.current_chunk_index].get_posting_list(self.current_doc_id_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compressor::compressor::CompressionAlgorithm;

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

    #[test]
    fn test_new_iterator_initialization() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200],
            vec![2, 3],
            vec![vec![1, 2], vec![3, 4, 5]],
        );
        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);
        let iterator = ChunkIterator::new(vec![decoded_chunk]);

        assert_eq!(iterator.current_chunk_index, 0);
        assert_eq!(iterator.current_doc_id_index, 0);
        assert_eq!(iterator.chunks.len(), 1);
    }

    #[test]
    fn test_init_decodes_first_chunk() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200],
            vec![2, 3],
            vec![vec![1, 2], vec![3, 4, 5]],
        );
        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);
        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();
        assert_eq!(iterator.current_doc_id_index, 0);
        assert_eq!(iterator.chunks[0].doc_ids, vec![100, 200]);
        assert_eq!(iterator.chunks[0].doc_frequencies, vec![2, 3]);
    }
    #[test]
    fn test_get_no_of_postings_multiple_chunks() {
        let mut chunk1 =
            create_test_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let encoded1 = chunk1.encode();
        let mut decoded_chunk1 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk1.decode(&encoded1[4..]);

        let mut chunk2 = create_test_chunk(
            1,
            vec![300, 400, 500],
            vec![1, 1, 2],
            vec![vec![4], vec![5], vec![6, 7]],
        );
        let encoded2 = chunk2.encode();
        let mut decoded_chunk2 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk2.decode(&encoded2[4..]);

        let iterator = ChunkIterator::new(vec![decoded_chunk1, decoded_chunk2]);

        assert_eq!(iterator.get_no_of_postings(), 5);
    }

    #[test]
    fn test_contains_doc_id_true() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        assert!(iterator.contains_doc_id(200));
    }

    #[test]
    fn test_contains_doc_id_false() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        assert!(!iterator.contains_doc_id(250));
    }

    #[test]
    fn test_next_within_chunk() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        assert_eq!(iterator.current_doc_id_index, 0);
        assert!(iterator.next());
        assert_eq!(iterator.current_doc_id_index, 1);
        assert!(iterator.next());
        assert_eq!(iterator.current_doc_id_index, 2);
    }

    #[test]
    fn test_next_across_chunks() {
        let mut chunk1 =
            create_test_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let encoded1 = chunk1.encode();
        let mut decoded_chunk1 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk1.decode(&encoded1[4..]);

        let mut chunk2 =
            create_test_chunk(1, vec![300, 400], vec![1, 2], vec![vec![4], vec![5, 6]]);
        let encoded2 = chunk2.encode();
        let mut decoded_chunk2 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk2.decode(&encoded2[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk1, decoded_chunk2]);
        iterator.init();
        assert_eq!(iterator.get_doc_id(), 100);
        assert!(iterator.next());
        assert_eq!(iterator.get_doc_id(), 200);
        assert!(iterator.next());
        assert_eq!(iterator.get_doc_id(), 300);
        assert!(iterator.next());
        assert_eq!(iterator.get_doc_id(), 400);
        assert!(!iterator.next());
    }

    #[test]
    fn test_has_next_true() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.has_next());
        iterator.next();
        assert!(!iterator.has_next());
    }

    #[test]
    fn test_has_next_across_chunks() {
        let mut chunk1 = create_test_chunk(1, vec![100], vec![1], vec![vec![1]]);
        let encoded1 = chunk1.encode();
        let mut decoded_chunk1 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk1.decode(&encoded1[4..]);

        let mut chunk2 = create_test_chunk(1, vec![200], vec![2], vec![vec![2, 3]]);
        let encoded2 = chunk2.encode();
        let mut decoded_chunk2 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk2.decode(&encoded2[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk1, decoded_chunk2]);
        iterator.init();

        assert!(iterator.has_next());
        assert!(iterator.next());
        assert!(!iterator.has_next());
    }

    #[test]
    fn test_get_doc_frequency() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![5, 10, 15],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        assert_eq!(iterator.get_doc_frequency(), 5);
        iterator.next();
        assert_eq!(iterator.get_doc_frequency(), 10);
        iterator.next();
        assert_eq!(iterator.get_doc_frequency(), 15);
    }

    #[test]
    fn test_get_posting_list() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200],
            vec![2, 3],
            vec![vec![1, 5], vec![10, 20, 30]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        assert_eq!(iterator.get_posting_list(), vec![1, 5]);
        iterator.next();
        assert_eq!(iterator.get_posting_list(), vec![10, 20, 30]);
    }

    #[test]
    fn test_advance_within_same_chunk() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        iterator.advance(150);

        assert_eq!(iterator.current_chunk_index, 0);
        assert_eq!(iterator.current_doc_id_index, 1);
        assert_eq!(iterator.get_doc_id(), 200);
    }

    #[test]
    fn test_advance_to_next_chunk() {
        let mut chunk1 =
            create_test_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let encoded1 = chunk1.encode();
        let mut decoded_chunk1 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk1.decode(&encoded1[4..]);

        let mut chunk2 =
            create_test_chunk(1, vec![300, 400], vec![1, 2], vec![vec![4], vec![5, 6]]);
        let encoded2 = chunk2.encode();
        let mut decoded_chunk2 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk2.decode(&encoded2[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk1, decoded_chunk2]);
        iterator.init();

        iterator.advance(350);

        assert_eq!(iterator.current_chunk_index, 1);
        assert_eq!(iterator.current_doc_id_index, 1);
        assert_eq!(iterator.get_doc_id(), 400);
    }

    #[test]
    fn test_advance_skips_multiple_chunks() {
        let mut chunk1 =
            create_test_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let encoded1 = chunk1.encode();
        let mut decoded_chunk1 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk1.decode(&encoded1[4..]);

        let mut chunk2 =
            create_test_chunk(1, vec![300, 400], vec![1, 2], vec![vec![4], vec![5, 6]]);
        let encoded2 = chunk2.encode();
        let mut decoded_chunk2 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk2.decode(&encoded2[4..]);

        let mut chunk3 =
            create_test_chunk(1, vec![500, 550], vec![1, 2], vec![vec![7], vec![8, 9]]);
        let encoded3 = chunk3.encode();
        let mut decoded_chunk3 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk3.decode(&encoded3[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk1, decoded_chunk2, decoded_chunk3]);
        iterator.init();

        iterator.advance(550);

        assert_eq!(iterator.current_chunk_index, 2);
        assert_eq!(iterator.get_doc_id(), 550)
    }

    #[test]
    fn test_full_iteration_single_chunk() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![1, 2, 3],
            vec![vec![1], vec![2, 3], vec![4, 5, 6]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        let mut doc_ids = vec![];
        doc_ids.push(iterator.get_doc_id());

        while iterator.next() {
            doc_ids.push(iterator.get_doc_id());
        }

        assert_eq!(doc_ids, vec![100, 200, 300]);
    }

    #[test]
    fn test_full_iteration_multiple_chunks() {
        let mut chunk1 =
            create_test_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let encoded1 = chunk1.encode();
        let mut decoded_chunk1 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk1.decode(&encoded1[4..]);

        let mut chunk2 =
            create_test_chunk(1, vec![300, 400], vec![1, 2], vec![vec![4], vec![5, 6]]);
        let encoded2 = chunk2.encode();
        let mut decoded_chunk2 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk2.decode(&encoded2[4..]);

        let mut chunk3 = create_test_chunk(1, vec![500], vec![1], vec![vec![7]]);
        let encoded3 = chunk3.encode();
        let mut decoded_chunk3 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk3.decode(&encoded3[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk1, decoded_chunk2, decoded_chunk3]);
        iterator.init();

        let mut doc_ids = vec![];
        let mut frequencies = vec![];

        doc_ids.push(iterator.get_doc_id());
        frequencies.push(iterator.get_doc_frequency());

        while iterator.next() {
            doc_ids.push(iterator.get_doc_id());
            frequencies.push(iterator.get_doc_frequency());
        }

        assert_eq!(doc_ids, vec![100, 200, 300, 400, 500]);
        assert_eq!(frequencies, vec![1, 2, 1, 2, 1]);
    }

    #[test]
    fn test_advance_then_iterate() {
        let mut chunk1 =
            create_test_chunk(1, vec![100, 200], vec![1, 2], vec![vec![1], vec![2, 3]]);
        let encoded1 = chunk1.encode();
        let mut decoded_chunk1 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk1.decode(&encoded1[4..]);

        let mut chunk2 =
            create_test_chunk(1, vec![300, 400], vec![3, 4], vec![vec![4], vec![5, 6]]);
        let encoded2 = chunk2.encode();
        let mut decoded_chunk2 = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk2.decode(&encoded2[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk1, decoded_chunk2]);
        iterator.init();

        iterator.advance(350);

        let mut doc_ids = vec![];
        doc_ids.push(iterator.get_doc_id());

        while iterator.next() {
            doc_ids.push(iterator.get_doc_id());
        }

        assert_eq!(doc_ids, vec![400]);
    }

    #[test]
    fn test_empty_chunks_vec() {
        let iterator = ChunkIterator::new(vec![]);
        assert_eq!(iterator.get_no_of_postings(), 0);
    }

    #[test]
    fn test_iterator_state_consistency() {
        let mut chunk = create_test_chunk(
            1,
            vec![100, 200, 300],
            vec![5, 10, 15],
            vec![vec![1, 2], vec![3, 4, 5], vec![6, 7, 8, 9]],
        );
        let encoded = chunk.encode();
        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        let mut iterator = ChunkIterator::new(vec![decoded_chunk]);
        iterator.init();

        // First position
        assert_eq!(iterator.get_doc_id(), 100);
        assert_eq!(iterator.get_doc_frequency(), 5);
        assert_eq!(iterator.get_posting_list(), vec![1, 2]);

        // Move and verify
        iterator.next();
        assert_eq!(iterator.get_doc_id(), 200);
        assert_eq!(iterator.get_doc_frequency(), 10);
        assert_eq!(iterator.get_posting_list(), vec![3, 4, 5]);

        // Move and verify
        iterator.next();
        assert_eq!(iterator.get_doc_id(), 300);
        assert_eq!(iterator.get_doc_frequency(), 15);
        assert_eq!(iterator.get_posting_list(), vec![6, 7, 8, 9]);
    }
}

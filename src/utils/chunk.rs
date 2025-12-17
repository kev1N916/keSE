use crate::compressor::compressor::{CompressionAlgorithm, Compressor};
const POSITIONS_DELIMITER: u8 = 0x00;

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub size_of_chunk: u32,                  // stored on disk
    pub max_doc_id: u32,                     // stored on disk
    pub no_of_postings: u8,                  // stored on disk
    pub compressed_doc_ids: Vec<u8>,         // stored on disk
    pub compressed_doc_frequencies: Vec<u8>, // stored on disk
    pub compressed_doc_positions: Vec<u8>,   // stored on disk
    pub indexed_compressed_positions: Vec<Vec<u8>>,
    pub compressor: Compressor,
    pub doc_ids: Vec<u32>,
    pub doc_positions: Vec<Vec<u32>>,
    pub doc_frequencies: Vec<u32>,
    pub term: u32,
    pub last_doc_id: u32,
}

impl Chunk {
    pub fn new(term: u32, compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            size_of_chunk: 9,
            max_doc_id: 0,
            no_of_postings: 0,
            last_doc_id: 0,
            compressed_doc_ids: Vec::new(),
            compressed_doc_positions: Vec::new(),
            compressed_doc_frequencies: Vec::new(),
            indexed_compressed_positions: Vec::new(),
            compressor: Compressor::new(compression_algorithm),
            term,
            doc_ids: Vec::new(),
            doc_frequencies: Vec::new(),
            doc_positions: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.size_of_chunk = 9;
        self.last_doc_id = 0;
        self.max_doc_id = 0;
        self.doc_positions.clear();
        self.doc_frequencies.clear();
        self.doc_ids.clear();
        self.no_of_postings = 0;
    }

    pub fn get_doc_ids(&self) -> Vec<u32> {
        if self.compressed_doc_ids.len() > 0 {
            return self
                .compressor
                .decompress_list_with_difference(&self.compressed_doc_ids);
        }
        self.doc_ids.clone()
    }

    pub fn get_no_of_postings(&self) -> u8 {
        self.no_of_postings
    }

    pub fn get_doc_frequencies(&self) -> Vec<u32> {
        if self.compressed_doc_frequencies.len() > 0 {
            return self
                .compressor
                .decompress_list(&self.compressed_doc_frequencies);
        }
        self.doc_frequencies.clone()
    }

    pub fn get_posting_list(&self, index: usize) -> Vec<u32> {
        if self.indexed_compressed_positions.len() > 0 {
            return self
                .compressor
                .decompress_list_with_difference(&self.indexed_compressed_positions[index]);
        }
        Vec::new()
    }

    pub fn decode_doc_ids(&mut self) {
        if self.compressed_doc_ids.len() > 0 {
            self.doc_ids = self
                .compressor
                .decompress_list_with_difference(&self.compressed_doc_ids);
            self.compressed_doc_ids.clear();
        }
    }

    pub fn decode_doc_frequencies(&mut self) {
        if self.compressed_doc_frequencies.len() > 0 {
            self.doc_frequencies = self
                .compressor
                .decompress_list(&self.compressed_doc_frequencies);
            self.compressed_doc_frequencies.clear();
        }
    }

    pub fn index_positions(&mut self) {
        if self.compressed_doc_positions.len() == 0 {
            return;
        }
        let mut posting_list: &[u8] = &[];
        let mut i = 0;
        let mut indexed_compressed_positions = Vec::new();
        while i < self.compressed_doc_positions.len() {
            let mut j = i;
            while j < self.compressed_doc_positions.len() && self.compressed_doc_positions[j] != 0 {
                j += 1;
            }
            if j == i {
                break;
            }
            posting_list = &self.compressed_doc_positions[i as usize..j as usize];
            i = j + 1;
            indexed_compressed_positions.push(posting_list.to_vec());
        }
        self.indexed_compressed_positions = indexed_compressed_positions;
    }

    pub fn add_doc_id(&mut self, doc_id: u32) {
        self.doc_ids.push(doc_id);
        self.set_max_doc_id(doc_id);
    }

    pub fn add_doc_positions(&mut self, positions: Vec<u32>) {
        self.doc_positions.push(positions)
    }

    pub fn add_doc_frequency(&mut self, doc_frequency: u32) {
        self.doc_frequencies.push(doc_frequency)
    }

    pub fn encode(&mut self) -> Vec<u8> {
        let mut chunk_bytes: Vec<u8> = Vec::new();
        chunk_bytes.extend_from_slice(&self.size_of_chunk.to_le_bytes());
        chunk_bytes.extend_from_slice(&self.no_of_postings.to_le_bytes());
        chunk_bytes.extend_from_slice(&self.max_doc_id.to_le_bytes());
        chunk_bytes.extend(&self.compressor.compress_list_with_difference(&self.doc_ids));
        chunk_bytes.push(POSITIONS_DELIMITER);
        chunk_bytes.extend(&self.compressor.compress_list(&self.doc_frequencies));
        chunk_bytes.push(POSITIONS_DELIMITER);

        if self.doc_positions.len() > 0 {
            for position in &self.doc_positions {
                chunk_bytes.extend(self.compressor.compress_list_with_difference(position));
                chunk_bytes.push(POSITIONS_DELIMITER);
            }
        }
        self.size_of_chunk = (chunk_bytes.len() - 4) as u32;
        chunk_bytes[0..4].copy_from_slice(&self.size_of_chunk.to_le_bytes());
        chunk_bytes
    }

    pub fn decode(&mut self, chunk_bytes: &[u8]) {
        self.size_of_chunk = (4 + chunk_bytes.len()) as u32;
        let mut offset = 0;
        self.no_of_postings =
            u8::from_le_bytes(chunk_bytes[offset..offset + 1].try_into().unwrap());
        if self.no_of_postings == 0 {
            return;
        }
        offset += 1;
        self.max_doc_id = u32::from_le_bytes(chunk_bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let mut doc_id_index = offset;
        while doc_id_index < chunk_bytes.len() {
            if chunk_bytes[doc_id_index] == 0 {
                break;
            }
            doc_id_index += 1;
        }
        self.compressed_doc_ids = chunk_bytes[offset..doc_id_index].to_vec();
        offset = doc_id_index + 1;
        let mut doc_frequncy_index = offset;
        while doc_frequncy_index < chunk_bytes.len() {
            if chunk_bytes[doc_frequncy_index] == 0 {
                break;
            }
            doc_frequncy_index += 1;
        }
        self.compressed_doc_frequencies = chunk_bytes[offset..doc_frequncy_index].to_vec();
        self.compressed_doc_positions = chunk_bytes[doc_frequncy_index + 1..].to_vec();
        self.index_positions();
    }

    pub fn set_max_doc_id(&mut self, doc_id: u32) {
        self.max_doc_id = self.max_doc_id.max(doc_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compressor::compressor::CompressionAlgorithm;

    #[test]
    fn test_new_chunk_initialization() {
        let chunk = Chunk::new(42, CompressionAlgorithm::VarByte);

        assert_eq!(chunk.term, 42);
        assert_eq!(chunk.size_of_chunk, 9);
        assert_eq!(chunk.max_doc_id, 0);
        assert_eq!(chunk.no_of_postings, 0);
        assert_eq!(chunk.last_doc_id, 0);
        assert!(chunk.doc_ids.is_empty());
        assert!(chunk.doc_frequencies.is_empty());
        assert!(chunk.doc_positions.is_empty());
    }

    #[test]
    fn test_encode_decode_empty_chunk() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::VarByte);

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]); // Skip size_of_chunk bytes

        assert_eq!(decoded_chunk.no_of_postings, 0);
        assert_eq!(decoded_chunk.max_doc_id, 0);
    }

    #[test]
    fn test_encode_decode_single_document() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::VarByte);

        chunk.add_doc_id(100);
        chunk.add_doc_frequency(5);
        chunk.add_doc_positions(vec![1, 5, 10, 15, 20]);
        chunk.set_max_doc_id(100);
        chunk.no_of_postings = 1;

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        assert_eq!(decoded_chunk.no_of_postings, 1);
        assert_eq!(decoded_chunk.max_doc_id, 100);
        assert_eq!(decoded_chunk.get_doc_ids(), vec![100]);
        assert_eq!(decoded_chunk.get_doc_frequencies(), vec![5]);
        assert_eq!(decoded_chunk.get_posting_list(0), vec![1, 5, 10, 15, 20]);
    }

    #[test]
    fn test_encode_decode_multiple_documents() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::VarByte);

        chunk.add_doc_id(100);
        chunk.add_doc_id(200);
        chunk.add_doc_id(300);

        chunk.add_doc_frequency(3);
        chunk.add_doc_frequency(2);
        chunk.add_doc_frequency(4);

        chunk.add_doc_positions(vec![1, 5, 10]);
        chunk.add_doc_positions(vec![20, 25]);
        chunk.add_doc_positions(vec![30, 35, 40, 45]);

        chunk.set_max_doc_id(300);
        chunk.no_of_postings = 3;

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        assert_eq!(decoded_chunk.no_of_postings, 3);
        assert_eq!(decoded_chunk.max_doc_id, 300);
        assert_eq!(decoded_chunk.get_doc_ids(), vec![100, 200, 300]);
        assert_eq!(decoded_chunk.get_doc_frequencies(), vec![3, 2, 4]);
        assert_eq!(decoded_chunk.get_posting_list(0), vec![1, 5, 10]);
        assert_eq!(decoded_chunk.get_posting_list(1), vec![20, 25]);
        assert_eq!(decoded_chunk.get_posting_list(2), vec![30, 35, 40, 45]);
    }

    #[test]
    fn test_encode_decode_no_positions() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::VarByte);

        chunk.add_doc_id(100);
        chunk.add_doc_id(200);
        chunk.add_doc_frequency(5);
        chunk.add_doc_frequency(3);
        chunk.set_max_doc_id(200);
        chunk.no_of_postings = 2;

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        assert_eq!(decoded_chunk.no_of_postings, 2);
        assert_eq!(decoded_chunk.max_doc_id, 200);
        assert_eq!(decoded_chunk.get_doc_ids(), vec![100, 200]);
        assert_eq!(decoded_chunk.get_doc_frequencies(), vec![5, 3]);
        assert!(decoded_chunk.indexed_compressed_positions.is_empty());
    }

    #[test]
    fn test_encode_decode_large_values() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::VarByte);

        chunk.add_doc_id(1000000);
        chunk.add_doc_id(2000000);
        chunk.add_doc_frequency(100);
        chunk.add_doc_frequency(200);
        chunk.add_doc_positions(vec![100, 200, 300, 400, 500]);
        chunk.add_doc_positions(vec![1000, 2000, 3000]);
        chunk.set_max_doc_id(2000000);
        chunk.no_of_postings = 2;

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);

        assert_eq!(decoded_chunk.get_doc_ids(), vec![1000000, 2000000]);
        assert_eq!(decoded_chunk.get_doc_frequencies(), vec![100, 200]);
        assert_eq!(
            decoded_chunk.get_posting_list(0),
            vec![100, 200, 300, 400, 500]
        );
        assert_eq!(decoded_chunk.get_posting_list(1), vec![1000, 2000, 3000]);
    }

    #[test]
    fn test_reset() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::VarByte);

        chunk.add_doc_id(100);
        chunk.add_doc_frequency(5);
        chunk.add_doc_positions(vec![1, 2, 3]);
        chunk.set_max_doc_id(100);
        chunk.no_of_postings = 1;

        chunk.reset();

        assert_eq!(chunk.size_of_chunk, 9);
        assert_eq!(chunk.last_doc_id, 0);
        assert_eq!(chunk.max_doc_id, 0);
        assert_eq!(chunk.no_of_postings, 0);
        assert!(chunk.doc_ids.is_empty());
        assert!(chunk.doc_frequencies.is_empty());
        assert!(chunk.doc_positions.is_empty());
    }

    #[test]
    fn test_size_of_chunk_calculation() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::VarByte);

        chunk.add_doc_id(100);
        chunk.add_doc_frequency(5);
        chunk.add_doc_positions(vec![1, 2, 3]);
        chunk.no_of_postings = 1;

        let encoded = chunk.encode();
        let size_from_bytes = u32::from_le_bytes(encoded[0..4].try_into().unwrap());

        // Size should be total length minus the 4 bytes for size field itself
        assert_eq!(size_from_bytes, (encoded.len() - 4) as u32);
        assert_eq!(chunk.size_of_chunk, (encoded.len() - 4) as u32);
    }

    #[test]
    fn test_roundtrip_consistency() {
        let mut original = Chunk::new(1, CompressionAlgorithm::VarByte);

        // Add diverse data
        for i in 0..5 {
            original.add_doc_id((i + 1) * 100);
            original.add_doc_frequency(i + 1);
            let positions: Vec<u32> = (0..=i).map(|j| (j + 1) * 10).collect();
            original.add_doc_positions(positions);
        }
        original.set_max_doc_id(500);
        original.no_of_postings = 5;

        let encoded = original.encode();

        let mut decoded = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded.decode(&encoded[4..]);

        // Verify all data matches
        assert_eq!(decoded.no_of_postings, original.no_of_postings);
        assert_eq!(decoded.max_doc_id, original.max_doc_id);
        assert_eq!(decoded.get_doc_ids(), original.doc_ids);
        assert_eq!(decoded.get_doc_frequencies(), original.doc_frequencies);

        for i in 0..original.doc_positions.len() {
            assert_eq!(decoded.get_posting_list(i), original.doc_positions[i]);
        }
    }
}

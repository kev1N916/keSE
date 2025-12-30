use crate::compressor::compressor::{CompressionAlgorithm, Compressor};

// The Chunk is a unit of storage for a posting list
// Each posting list is divided into chunks.
// The maximum size of a chunk is 128 postings.
// When we store the chunk on disk, we store the chunk size in bytes,
// the no of postings in this chunk(it may have less than 128 postings), the max document id
// stored in the chunk and then the compressed doc_ids, the compressed frequenices
// and then the compressed positions if we are choosing to store positions.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub size_of_chunk: u32,
    pub max_doc_id: u32,
    pub no_of_postings: u8,
    pub compressed_doc_ids: Vec<u8>,
    pub compressed_doc_frequencies: Vec<u8>,
    pub compressed_doc_positions: Vec<u8>,
    pub indexed_compressed_positions: Vec<Vec<u8>>,
    pub compressor: Compressor,
    pub p_for_delta_compressor: Compressor,
    pub doc_ids: Vec<u32>,
    pub doc_positions: Vec<Vec<u32>>,
    pub doc_frequencies: Vec<u32>,
    pub term: u32,
}

impl Chunk {
    pub fn new(term: u32, compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            // the default size of the chunk is 9
            // ( 4 for the max_doc_id and size_of_chunk and 1 byte for no_of_postings)
            size_of_chunk: 9,
            max_doc_id: 0,
            no_of_postings: 0,
            compressed_doc_ids: Vec::new(),
            compressed_doc_positions: Vec::new(),
            compressed_doc_frequencies: Vec::new(),
            indexed_compressed_positions: Vec::new(),
            compressor: Compressor::new(compression_algorithm),
            p_for_delta_compressor: Compressor::new(CompressionAlgorithm::Simple16),
            term,
            doc_ids: Vec::new(),
            doc_frequencies: Vec::new(),
            doc_positions: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.size_of_chunk = 9;
        self.max_doc_id = 0;
        self.doc_positions.clear();
        self.doc_frequencies.clear();
        self.doc_ids.clear();
        self.no_of_postings = 0;
    }

    // pub fn get_doc_ids(&mut self) -> &[u32] {
    //     if self.compressed_doc_ids.len() > 0 {
    //         let mut doc_ids = self
    //             .compressor
    //             .decompress_list_with_dgaps(&self.compressed_doc_ids);
    //         doc_ids.truncate(self.no_of_postings as usize);
    //         self.doc_ids = doc_ids;
    //         self.compressed_doc_ids.clear();
    //     }
    //     self.doc_ids.as_slice()
    // }
    //
    //  // pub fn get_doc_frequencies(&mut self) -> &Vec<u32> {
    //     if self.compressed_doc_frequencies.len() > 0 {
    //         let mut doc_freq = self
    //             .compressor
    //             .decompress_list(&self.compressed_doc_frequencies);
    //         doc_freq.truncate(self.no_of_postings as usize);
    //         self.doc_frequencies = doc_freq;
    //         self.compressed_doc_frequencies.clear();
    //     }
    //     &self.doc_frequencies
    // }

    pub fn get_no_of_postings(&self) -> u8 {
        self.no_of_postings
    }

    pub fn get_posting_list(&self, index: usize) -> Vec<u32> {
        if self.indexed_compressed_positions.len() > 0 {
            let mut positions = self
                .compressor
                .decompress_list_with_dgaps(&self.indexed_compressed_positions[index]);
            positions.truncate(self.doc_frequencies[index] as usize);
            return positions;
        }
        Vec::new()
    }

    pub fn decode_doc_ids(&mut self) {
        if self.compressed_doc_ids.len() > 0 {
            if self.no_of_postings == 128 {
                self.doc_ids = self
                    .p_for_delta_compressor
                    .decompress_list_with_dgaps(&self.compressed_doc_ids);
            } else {
                self.doc_ids = self
                    .compressor
                    .decompress_list_with_dgaps(&self.compressed_doc_ids);
            }
            self.doc_ids.truncate(self.no_of_postings as usize);
            self.compressed_doc_ids.clear();
        }
    }

    pub fn decode_doc_frequencies(&mut self) {
        if self.compressed_doc_frequencies.len() > 0 {
            if self.no_of_postings == 128 {
                self.doc_frequencies = self
                    .p_for_delta_compressor
                    .decompress_list(&self.compressed_doc_frequencies);
            } else {
                self.doc_frequencies = self
                    .compressor
                    .decompress_list(&self.compressed_doc_frequencies);
            }
            self.doc_frequencies.truncate(self.no_of_postings as usize);
            self.compressed_doc_frequencies.clear();
        }
    }

    // we divide the compressed_doc_positions into individual compressed segments
    // so that retrieval of these segments is easier
    pub fn index_positions(&mut self) {
        if self.compressed_doc_positions.len() == 0 {
            return;
        }
        let mut offset = 0;
        self.indexed_compressed_positions.clear();
        for _ in 0..self.no_of_postings {
            let positions_length = u16::from_le_bytes(
                self.compressed_doc_positions[offset..offset + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            offset += 2;
            self.indexed_compressed_positions
                .push(self.compressed_doc_positions[offset..offset + positions_length].to_vec());
            offset += positions_length;
        }
        self.indexed_compressed_positions.shrink_to_fit();
        self.compressed_doc_positions.clear();
    }

    pub fn add_doc_id(&mut self, doc_id: u32) {
        self.doc_ids.push(doc_id);
        self.set_max_doc_id(doc_id);
        self.no_of_postings += 1;
    }

    pub fn add_doc_positions(&mut self, positions: Vec<u32>) {
        self.doc_positions.push(positions)
    }

    pub fn add_doc_frequency(&mut self, doc_frequency: u32) {
        self.doc_frequencies.push(doc_frequency)
    }

    pub fn encode(&mut self) -> Vec<u8> {
        let mut chunk_bytes: Vec<u8> = Vec::with_capacity(1000);
        chunk_bytes.extend_from_slice(&[0u8; 4]);
        chunk_bytes.extend_from_slice(&self.no_of_postings.to_le_bytes());
        chunk_bytes.extend_from_slice(&self.max_doc_id.to_le_bytes());
        if self.no_of_postings == 128 {
            let doc_id_bytes = self
                .p_for_delta_compressor
                .compress_list_with_d_gaps(&self.doc_ids);
            chunk_bytes.extend_from_slice(&(doc_id_bytes.len() as u16).to_le_bytes());
            chunk_bytes.extend(doc_id_bytes);
            let doc_freq_bytes = self
                .p_for_delta_compressor
                .compress_list(&self.doc_frequencies);
            chunk_bytes.extend_from_slice(&(doc_freq_bytes.len() as u16).to_le_bytes());
            chunk_bytes.extend(doc_freq_bytes);
        } else {
            let doc_id_bytes = self.compressor.compress_list_with_d_gaps(&self.doc_ids);
            chunk_bytes.extend_from_slice(&(doc_id_bytes.len() as u16).to_le_bytes());
            chunk_bytes.extend(doc_id_bytes);
            let doc_freq_bytes = self.compressor.compress_list(&self.doc_frequencies);
            chunk_bytes.extend_from_slice(&(doc_freq_bytes.len() as u16).to_le_bytes());
            chunk_bytes.extend(doc_freq_bytes);
        }
        if !self.doc_positions.is_empty() {
            for position in &self.doc_positions {
                let position_bytes = self.compressor.compress_list_with_d_gaps(position);
                chunk_bytes.extend_from_slice(&(position_bytes.len() as u16).to_le_bytes());
                chunk_bytes.extend(position_bytes);
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
        let doc_id_bytes_length =
            u16::from_le_bytes(chunk_bytes[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        self.compressed_doc_ids = chunk_bytes[offset..offset + doc_id_bytes_length].to_vec();
        offset += doc_id_bytes_length;
        let doc_freq_bytes_length =
            u16::from_le_bytes(chunk_bytes[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        self.compressed_doc_frequencies =
            chunk_bytes[offset..offset + doc_freq_bytes_length].to_vec();
        offset += doc_freq_bytes_length;
        self.compressed_doc_positions = chunk_bytes[offset..].to_vec();
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

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);
        decoded_chunk.decode_doc_ids();
        decoded_chunk.decode_doc_frequencies();
        assert_eq!(decoded_chunk.no_of_postings, 1);
        assert_eq!(decoded_chunk.max_doc_id, 100);
        assert_eq!(decoded_chunk.doc_ids, vec![100]);
        assert_eq!(decoded_chunk.doc_frequencies, vec![5]);
        assert_eq!(decoded_chunk.get_posting_list(0), vec![1, 5, 10, 15, 20]);
    }

    #[test]
    fn test_encode_decode_multiple_documents() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::Simple16);

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

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::Simple16);
        decoded_chunk.decode(&encoded[4..]);

        assert_eq!(decoded_chunk.no_of_postings, 3);
        assert_eq!(decoded_chunk.max_doc_id, 300);
        decoded_chunk.decode_doc_frequencies();
        decoded_chunk.decode_doc_ids();
        assert_eq!(decoded_chunk.doc_ids, vec![100, 200, 300]);
        assert_eq!(decoded_chunk.doc_frequencies, vec![3, 2, 4]);
        assert_eq!(decoded_chunk.get_posting_list(0), vec![1, 5, 10]);
        assert_eq!(decoded_chunk.get_posting_list(1), vec![20, 25]);
        assert_eq!(decoded_chunk.get_posting_list(2), vec![30, 35, 40, 45]);
    }

    #[test]
    fn test_encode_decode_no_positions() {
        let mut chunk = Chunk::new(1, CompressionAlgorithm::Simple9);

        chunk.add_doc_id(100);
        chunk.add_doc_id(200);
        chunk.add_doc_frequency(5);
        chunk.add_doc_frequency(3);
        chunk.set_max_doc_id(200);

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::Simple9);
        decoded_chunk.decode(&encoded[4..]);
        decoded_chunk.decode_doc_frequencies();
        decoded_chunk.decode_doc_ids();
        assert_eq!(decoded_chunk.no_of_postings, 2);
        assert_eq!(decoded_chunk.max_doc_id, 200);
        assert_eq!(decoded_chunk.doc_ids, vec![100, 200]);
        assert_eq!(decoded_chunk.doc_frequencies, vec![5, 3]);
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

        let encoded = chunk.encode();

        let mut decoded_chunk = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded_chunk.decode(&encoded[4..]);
        decoded_chunk.decode_doc_ids();
        decoded_chunk.decode_doc_frequencies();

        assert_eq!(decoded_chunk.doc_ids, vec![1000000, 2000000]);
        assert_eq!(decoded_chunk.doc_ids, vec![100, 200]);
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

        chunk.reset();

        assert_eq!(chunk.size_of_chunk, 9);
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

        let encoded = original.encode();

        let mut decoded = Chunk::new(1, CompressionAlgorithm::VarByte);
        decoded.decode(&encoded[4..]);
        decoded.decode_doc_frequencies();
        decoded.decode_doc_ids();
        // Verify all data matches
        assert_eq!(decoded.no_of_postings, original.no_of_postings);
        assert_eq!(decoded.max_doc_id, original.max_doc_id);
        assert_eq!(decoded.doc_ids, original.doc_ids);
        assert_eq!(decoded.doc_frequencies, original.doc_frequencies);

        for i in 0..original.doc_positions.len() {
            assert_eq!(decoded.get_posting_list(i), original.doc_positions[i]);
        }
    }
}

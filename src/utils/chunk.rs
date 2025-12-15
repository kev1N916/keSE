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
        self.compressor.decompress_list(&self.compressed_doc_ids)
    }

    pub fn get_no_of_postings(&self) -> u8 {
        self.no_of_postings
    }

    pub fn get_doc_frequencies(&self) -> Vec<u32> {
        self.compressor
            .decompress_list(&self.compressed_doc_frequencies)
    }

    pub fn get_posting_list(&self, index: usize) -> Vec<u32> {
        self.compressor
            .decompress_list(&self.indexed_compressed_positions[index])
    }

    pub fn decode_doc_ids(&mut self) {
        self.doc_ids = self.compressor.decompress_list(&self.compressed_doc_ids);
    }

    pub fn decode_doc_frequencies(&mut self) {
        self.doc_frequencies = self
            .compressor
            .decompress_list(&self.compressed_doc_frequencies);
    }

    pub fn index_positions(&mut self) {
        if self.compressed_doc_positions.len() == 0 {
            return;
        }
        let mut posting_list: &[u8] = &[];
        let mut i = 0;
        while i < self.doc_positions.len() {
            let mut j = i;
            while self.compressed_doc_positions[j] != 0 {
                j += 1;
            }
            posting_list = &self.compressed_doc_positions[i as usize..j as usize];
            i = j + 1;
            self.indexed_compressed_positions
                .push(posting_list.to_vec());
        }
        self.compressed_doc_positions.clear();
    }

    pub fn add_doc_id(&mut self, doc_id: u32) {
        self.doc_ids.push(doc_id)
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

        chunk_bytes.extend(&self.compressor.compress_list(&self.doc_ids));
        chunk_bytes.push(POSITIONS_DELIMITER);
        chunk_bytes.extend(&self.compressor.compress_list(&self.doc_frequencies));
        chunk_bytes.push(POSITIONS_DELIMITER);

        if self.doc_positions.len() > 0 {
            for position in &self.doc_positions {
                chunk_bytes.extend(self.compressor.compress_list(position));
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

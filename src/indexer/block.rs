use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader, Read, Seek},
};

use crate::indexer::chunk::Chunk;

const BLOCK_SIZE: usize = 64000;
pub struct Block {
    pub current_block_size: u32,
    pub no_of_terms: u32,
    pub block_id: u32,
    pub current_chunk: Chunk,
    pub chunks: Vec<Chunk>,
    pub block_bytes: [u8; BLOCK_SIZE],
    pub terms: Vec<u32>,
    pub term_offsets: Vec<u16>,
}

impl Block {
    pub fn new(block_id: u32) -> Self {
        Self {
            current_block_size: 4,
            no_of_terms: 0,
            block_id,
            current_chunk: Chunk::new(0),
            chunks: Vec::new(),
            block_bytes: [0; BLOCK_SIZE],
            term_offsets: Vec::new(),
            terms: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.current_block_size = 4;
        self.chunks.clear();
        self.terms.clear();
        self.block_bytes = [0; BLOCK_SIZE];
        self.term_offsets.clear();
    }

    pub fn check_if_term_exists(&self, term_id: u32) -> i64 {
        if let Ok(index) = self.terms.binary_search(&term_id) {
            return (index as u32).into();
        }
        -1
    }

    pub fn space_used(&self) -> u32 {
        self.current_block_size + 1 + self.current_chunk.size_of_chunk
    }
    pub fn set_block_id(&mut self, block_id: u32) {
        self.block_id = block_id;
    }
    pub fn set_no_of_terms(&mut self, no_of_terms: u32) {
        self.no_of_terms = no_of_terms;
    }
    pub fn reset_current_chunk(&mut self) {
        self.current_chunk.reset();
    }
    pub fn add_current_chunk(&mut self) {
        println!("{:?}", self.current_chunk);
        self.chunks.push(self.current_chunk.clone());
        self.current_block_size += self.current_chunk.size_of_chunk;
    }

    pub fn add_term(&mut self, term: u32) {
        self.current_block_size += 6;
        self.terms.push(term);
    }

    pub fn get_chunk_for_doc<'a>(&self, doc_id: u32, chunks: &'a [Chunk]) -> Option<&'a Chunk> {
        let mut i = 0;
        while i < chunks.len() {
            if chunks[i].max_doc_id < doc_id {
                i += 1;
            } else {
                break;
            }
        }
        if i == chunks.len() {
            return None;
        }
        Some(&chunks[i])
    }

    pub fn encode(&mut self) {
        let no_of_terms: [u8; 4] = (self.terms.len() as u32).to_le_bytes();
        let encoded_terms: Vec<u8> = self.terms.iter().flat_map(|&n| n.to_le_bytes()).collect();
        let mut term_offsets: Vec<u8> = Vec::new();
        let mut encoded_chunks: Vec<u8> = Vec::new();
        let mut term_offset_start = (6 * self.terms.len() + 4) as u16;
        let mut term_set = HashSet::new();
        for chunk in &self.chunks {
            if !term_set.contains(&chunk.term) {
                term_set.insert(chunk.term);
                let bytes = term_offset_start.to_le_bytes();
                term_offsets.extend(bytes);
            }
            encoded_chunks.extend(&chunk.encode());
            term_offset_start += (chunk.doc_ids.len() + chunk.positions.len() + 8 + 1) as u16;
        }
        let mut offset = 0;
        self.block_bytes[offset..offset + no_of_terms.len()].copy_from_slice(&no_of_terms);
        offset += no_of_terms.len();
        self.block_bytes[offset..offset + encoded_terms.len()].copy_from_slice(&encoded_terms);
        offset += encoded_terms.len();
        self.block_bytes[offset..offset + term_offsets.len()].copy_from_slice(&term_offsets);
        offset += term_offsets.len();
        self.block_bytes[offset..offset + encoded_chunks.len()].copy_from_slice(&encoded_chunks);
    }

    pub fn decode_chunks_for_term(&self, term_id: u32, term_index: usize) -> Vec<Chunk> {
        let mut chunk_vec: Vec<Chunk> = Vec::new();
        let term_offset_start = self.term_offsets[term_index] as usize;
        let term_off_end = if term_index == self.terms.len() - 1 {
            BLOCK_SIZE
        } else {
            self.term_offsets[term_index + 1] as usize
        };

        let chunk_bytes = &self.block_bytes[term_offset_start..term_off_end];
        let mut chunk_offset = 0;
        let mut current_chunk = Chunk::new(term_id);
        while chunk_offset < chunk_bytes.len() {
            let chunk_size = u32::from_le_bytes(
                chunk_bytes[chunk_offset..chunk_offset + 4]
                    .try_into()
                    .unwrap(),
            );
            if chunk_size == 0 {
                break;
            }
            current_chunk
                .decode(&chunk_bytes[chunk_offset + 4..chunk_offset + chunk_size as usize]);
            chunk_vec.push(current_chunk.clone());
            chunk_offset += chunk_size as usize;
        }
        chunk_vec
    }

    pub fn init(&mut self, reader: &mut BufReader<&mut File>) -> io::Result<()> {
        let _ = reader.seek(std::io::SeekFrom::Start(
            (self.block_id * BLOCK_SIZE as u32).into(),
        ))?;
        let _ = reader.read_exact(&mut self.block_bytes)?;
        let no_of_terms_in_block = u32::from_le_bytes(self.block_bytes[0..4].try_into().unwrap());
        self.no_of_terms = no_of_terms_in_block;
        let mut offset = 4;
        let mut terms: Vec<u32> = Vec::new();
        for _ in 0..no_of_terms_in_block {
            let term_id =
                u32::from_le_bytes(self.block_bytes[offset..offset + 4].try_into().unwrap());
            terms.push(term_id);
            offset += 4;
        }
        let mut term_offsets: Vec<u16> = Vec::new();
        for _ in 0..no_of_terms_in_block {
            let term_offset =
                u16::from_le_bytes(self.block_bytes[offset..offset + 2].try_into().unwrap());
            term_offsets.push(term_offset);
            offset += 2;
        }

        self.term_offsets = term_offsets;
        self.terms = terms;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dictionary::Posting,
        indexer::{helper::vb_decode_positions, index_merge_writer::MergedIndexBlockWriter},
    };
    use tempfile::NamedTempFile;

    // Helper function to create test postings
    fn create_test_postings(doc_id: u32, positions: Vec<u32>) -> Posting {
        Posting { doc_id, positions }
    }

    #[test]
    fn test_add_single_term_small_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = MergedIndexBlockWriter::new(file, Some(64), true);

        let postings = vec![
            create_test_postings(10, vec![5, 10, 15]),
            create_test_postings(20, vec![3, 7]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);
        // Check term metadata was updated
        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.block_ids.len(), 1);
        let mut block = Block::new(metadata.block_ids[0]);
        block.init(&mut reader).unwrap();
        assert_eq!(block.no_of_terms, 1);
        assert_eq!(block.terms, vec![1]);
    }

    #[test]
    fn test_add_multiple_terms() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = MergedIndexBlockWriter::new(file, Some(64), true);

        let postings1 = vec![create_test_postings(10, vec![1])];
        let postings2 = vec![create_test_postings(20, vec![2])];

        writer.add_term(1, postings1).unwrap();
        writer.add_term(2, postings2).unwrap();
        writer.finish().unwrap();

        let metadata1 = writer.get_term_metadata(1).unwrap();
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);
        // Check term metadata was updated
        let mut block = Block::new(metadata1.block_ids[0]);
        block.init(&mut reader).unwrap();
        assert_eq!(block.no_of_terms, 2);
        assert_eq!(block.terms, vec![1, 2]);
    }

    #[test]
    fn test_multiple_blocks_same_term() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = MergedIndexBlockWriter::new(file, Some(64), true);

        let postings = vec![
            create_test_postings(10, vec![5, 10, 15]),
            create_test_postings(20, vec![3, 7]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);
        // Check term metadata was updated
        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.block_ids.len(), 1);
        let mut block = Block::new(metadata.block_ids[0]);
        block.init(&mut reader).unwrap();
        assert_eq!(block.no_of_terms, 1);
        assert_eq!(block.terms, vec![1]);
    }

    #[test]
    fn test_sparse_doc_ids() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = MergedIndexBlockWriter::new(file, Some(64), true);

        let postings1 = vec![
            create_test_postings(10, vec![1, 6, 7, 13, 20]),
            create_test_postings(1000, vec![2, 6, 8, 9]),
            create_test_postings(10000, vec![3, 5]),
            create_test_postings(100000, vec![4, 5, 6, 9, 10]),
        ];

        let postings2 = vec![
            create_test_postings(12, vec![1, 6, 7, 13, 20]),
            create_test_postings(14, vec![2, 6, 8, 9]),
            create_test_postings(90, vec![3, 5, 7, 19, 22, 49]),
            create_test_postings(100, vec![4, 5, 6, 9, 10]),
        ];

        writer.add_term(1, postings1).unwrap();
        writer.add_term(2, postings2).unwrap();
        writer.finish().unwrap();
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);
        let metadata = writer.get_term_metadata(1).unwrap();
        let mut block = Block::new(metadata.block_ids[0]);
        block.init(&mut reader).unwrap();
        let chunks = block.decode_chunks_for_term(1, 0);
        let doc_ids = chunks[0].get_doc_ids();
        assert_eq!(doc_ids, vec![10, 1000, 10000, 100000]);
        let postings1 = chunks[0].get_posting_list(0);
        assert_eq!(postings1, vec![1, 6, 7, 13, 20]);
        let postings2 = chunks[0].get_posting_list(1);
        assert_eq!(postings2, vec![2, 6, 8, 9]);
        let postings3 = chunks[0].get_posting_list(2);
        assert_eq!(postings3, vec![3, 5]);
        let postings4 = chunks[0].get_posting_list(3);
        assert_eq!(postings4, vec![4, 5, 6, 9, 10]);

        let chunks = block.decode_chunks_for_term(2, 1);
        let doc_ids = vb_decode_positions(&chunks[0].doc_ids);
        assert_eq!(doc_ids, vec![12, 14, 90, 100]);
        let postings1 = chunks[0].get_posting_list(0);
        assert_eq!(postings1, vec![1, 6, 7, 13, 20]);
        let postings2 = chunks[0].get_posting_list(1);
        assert_eq!(postings2, vec![2, 6, 8, 9]);
        let postings3 = chunks[0].get_posting_list(2);
        assert_eq!(postings3, vec![3, 5, 7, 19, 22, 49]);
        let postings4 = chunks[0].get_posting_list(3);
        assert_eq!(postings4, vec![4, 5, 6, 9, 10]);
    }

    #[test]
    fn test_multiple_blocks_same_term_2() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = MergedIndexBlockWriter::new(file, Some(1), true); // Very small blocks

        // Create enough postings to span multiple blocks
        let mut postings = Vec::new();
        for i in 0..200 {
            postings.push(create_test_postings(i * 100, vec![1, 2, 3, 4, 5]));
        }

        let result = writer.add_term(1, postings);

        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 200);
        // Term should appear in multiple blocks due to small block size
        assert!(metadata.block_ids.len() > 1);

        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);
        let mut block1 = Block::new(metadata.block_ids[0]);
        block1.init(&mut reader).unwrap();
        let chunks1 = block1.decode_chunks_for_term(1, 0);
        let mut postings_read = Vec::new();
        for chunk in chunks1 {
            let doc_ids = vb_decode_positions(&chunk.doc_ids);
            for index in 0..doc_ids.len() {
                postings_read.push(Posting {
                    doc_id: doc_ids[index],
                    positions: chunk.get_posting_list(index),
                });
            }
        }
        let mut block2 = Block::new(metadata.block_ids[1]);
        block2.init(&mut reader).unwrap();
        let chunks2 = block2.decode_chunks_for_term(1, 0);
        for chunk in chunks2 {
            let doc_ids = vb_decode_positions(&chunk.doc_ids);
            for index in 0..doc_ids.len() {
                postings_read.push(Posting {
                    doc_id: doc_ids[index],
                    positions: chunk.get_posting_list(index),
                });
            }
        }
        let mut old_postings = Vec::new();
        for i in 0..200 {
            old_postings.push(create_test_postings(i * 100, vec![1, 2, 3, 4, 5]));
        }
        assert_eq!(old_postings, postings_read);
    }
}

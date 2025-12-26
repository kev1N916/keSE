use std::{
    fs::File,
    io::{self, BufReader, Read, Seek},
};

use crate::{compressor::compressor::CompressionAlgorithm, utils::chunk::Chunk};

/*
 The unit of storage in our inverted index is a block.
 Each term can span across multiple blocks and so we need to keep track of the block ids for
 each term so that we can process queries.
 The block layout consists first of the block metadata which includes the no of terms in the block and
 then the term and the term offsets.
 Following this metadata we have the actual chunks.
 We use the term offsets to index into the chunks for a particular term.
*/
pub struct Block {
    pub max_block_size: u8,      // maximum size of block in kb
    pub current_block_size: u32, // the current block size
    pub block_id: u32,
    pub chunk_bytes: Vec<u8>, // constains all the encoded chunks which are present in this block
    // Block Metadata
    pub no_of_terms: u32,       // total number of terms stored in the block
    pub terms: Vec<u32>,        // the terms which are present in the block
    pub term_offsets: Vec<u16>, // the offset from where the chunks of the term starts
}

impl Block {
    pub fn new(block_id: u32, max_block_size: Option<u8>) -> Self {
        Self {
            max_block_size: max_block_size.unwrap_or_else(|| 64),
            current_block_size: 4,
            no_of_terms: 0,
            block_id,
            chunk_bytes: Vec::new(),
            term_offsets: Vec::new(),
            terms: Vec::new(),
        }
    }

    // the default block size is 4 as we will always store the no_of_terms as 4 bytes in the block
    pub fn reset(&mut self) {
        self.current_block_size = 4;
        self.no_of_terms = 0;
        self.terms.clear();
        self.chunk_bytes.clear();
        self.term_offsets.clear();
    }

    pub fn check_if_term_exists(&self, term_id: u32) -> i64 {
        if let Ok(index) = self.terms.binary_search(&term_id) {
            return (index as u32).into();
        }
        -1
    }

    pub fn set_block_id(&mut self, block_id: u32) {
        self.block_id = block_id;
    }
    pub fn set_no_of_terms(&mut self, no_of_terms: u32) {
        self.no_of_terms = no_of_terms;
    }

    // when we add a term we also add the the term offset which is basically the current length of
    // chunk_bytes,which is where the chunks of that term will start
    pub fn add_term(&mut self, term: u32) {
        self.current_block_size += 6;
        self.terms.push(term);
        self.term_offsets.push(self.chunk_bytes.len() as u16);
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

    // since max_block_size is in kb, multiply by 1000
    pub fn space_left(&self) -> u32 {
        self.max_block_size as u32 * 1000 as u32 - self.current_block_size
    }

    pub fn add_chunk_bytes(&mut self, chunk_bytes: Vec<u8>) {
        self.chunk_bytes.extend_from_slice(&chunk_bytes);
        self.current_block_size += chunk_bytes.len() as u32;
    }

    // The chunks of the term will span accross its term offset and the next term offset
    // if the term is at the end of this block then we will keep decoding chunks until
    // we get a chunk which has a size of 0.
    pub fn decode_chunks_for_term(
        &self,
        term_id: u32,
        term_index: usize,
        compression_algorithm: CompressionAlgorithm,
    ) -> Vec<Chunk> {
        let mut chunk_vec: Vec<Chunk> = Vec::new();
        let term_offset_start = self.term_offsets[term_index] as usize;
        let term_off_end = if term_index == self.terms.len() - 1 {
            self.chunk_bytes.len()
        } else {
            self.term_offsets[term_index + 1] as usize
        };

        let chunk_bytes = &self.chunk_bytes[term_offset_start..term_off_end];
        let mut chunk_offset = 0;
        let mut current_chunk = Chunk::new(term_id, compression_algorithm);
        while chunk_offset + 4 < chunk_bytes.len() {
            let chunk_size = u32::from_le_bytes(
                chunk_bytes[chunk_offset..chunk_offset + 4]
                    .try_into()
                    .unwrap(),
            );
            // if we are trying to get the chunks for the last term present in the block we stop when we reach
            // a chunk of size 0
            if chunk_size == 0 {
                break;
            }
            chunk_offset += 4;
            current_chunk.decode(&chunk_bytes[chunk_offset..chunk_offset + chunk_size as usize]);
            chunk_vec.push(current_chunk.clone());
            chunk_offset += chunk_size as usize;
        }
        chunk_vec
    }

    // We store the no of terms, the terms, the term offsets and then the chunk_bytes
    pub fn encode(&mut self, block_bytes: &mut Vec<u8>) {
        assert_eq!(self.term_offsets.len(), self.terms.len());
        block_bytes.resize(self.max_block_size as usize * 1000, 0);
        let mut offset = 0;
        block_bytes[offset..offset + 4].copy_from_slice(&(self.terms.len() as u32).to_le_bytes());
        offset += 4;
        let encoded_terms: Vec<u8> = self.terms.iter().flat_map(|&n| n.to_le_bytes()).collect();
        block_bytes[offset..offset + encoded_terms.len()].copy_from_slice(&encoded_terms);
        offset += encoded_terms.len();
        let encoded_term_offsets: Vec<u8> = self
            .term_offsets
            .iter()
            .flat_map(|&n| n.to_le_bytes())
            .collect();
        block_bytes[offset..offset + encoded_term_offsets.len()]
            .copy_from_slice(&encoded_term_offsets);
        offset += encoded_term_offsets.len();
        block_bytes[offset..offset + self.chunk_bytes.len()].copy_from_slice(&self.chunk_bytes);
    }

    pub fn decode(&mut self, reader: &mut BufReader<&mut File>) -> io::Result<()> {
        let block_size = self.max_block_size as usize * 1000;
        reader.seek(std::io::SeekFrom::Start(
            (self.block_id * block_size as u32).into(),
        ))?;
        let mut block_bytes: Vec<u8> = vec![0; block_size];
        reader.read(&mut block_bytes).unwrap();
        self.no_of_terms = u32::from_le_bytes(block_bytes[0..4].try_into().unwrap());
        let mut offset = 4;
        self.terms.clear();
        for _ in 0..self.no_of_terms {
            self.terms.push(u32::from_le_bytes(
                block_bytes[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
        }
        self.term_offsets.clear();
        for _ in 0..self.no_of_terms {
            self.term_offsets.push(u16::from_le_bytes(
                block_bytes[offset..offset + 2].try_into().unwrap(),
            ));
            offset += 2;
        }
        self.chunk_bytes = block_bytes[offset..].to_vec();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{indexer::spimi::spimi_merge_writer::SpimiMergeWriter, utils::posting::Posting};

    use super::*;
    use std::io::BufReader;
    use tempfile::NamedTempFile;

    fn create_test_posting(doc_id: u32, positions: Vec<u32>) -> Posting {
        Posting { doc_id, positions }
    }

    #[test]
    fn test_add_single_term_verify_block_content() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::VarByte);

        let postings = vec![
            create_test_posting(10, vec![5, 10, 15]),
            create_test_posting(20, vec![3, 7]),
        ];

        writer.add_term(61, postings).unwrap();
        writer.finish().unwrap();

        // Read back and verify
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata = writer.get_term_metadata(61).unwrap();
        let mut block = Block::new(metadata.block_ids[0], Some(64));
        block.decode(&mut reader).unwrap();

        assert_eq!(block.no_of_terms, 1);
        assert_eq!(block.terms, vec![61]);

        let chunks = block.decode_chunks_for_term(61, 0, CompressionAlgorithm::VarByte);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].doc_ids.len(), 0);
    }

    #[test]
    fn test_add_multiple_terms_verify_block() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::VarByte);

        let postings1 = vec![create_test_posting(10, vec![1])];
        let postings2 = vec![create_test_posting(20, vec![2])];

        writer.add_term(1, postings1).unwrap();
        writer.add_term(2, postings2).unwrap();
        writer.finish().unwrap();

        // Read back and verify
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata1 = writer.get_term_metadata(1).unwrap();
        let mut block = Block::new(metadata1.block_ids[0], Some(64));
        block.decode(&mut reader).unwrap();

        assert_eq!(block.no_of_terms, 2);
        assert_eq!(block.terms, vec![1, 2]);
    }

    #[test]
    fn test_sparse_doc_ids() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(10),
            Some(64),
            true,
            CompressionAlgorithm::VarByte,
        );

        let postings1 = vec![
            create_test_posting(10, vec![1, 6, 7, 13, 20]),
            create_test_posting(1000, vec![2, 6, 8, 9]),
            create_test_posting(10000, vec![3, 5]),
            create_test_posting(100000, vec![4, 5, 6, 9, 10]),
        ];

        let postings2 = vec![
            create_test_posting(12, vec![1, 6, 7, 13, 20]),
            create_test_posting(14, vec![2, 6, 8, 9]),
            create_test_posting(90, vec![3, 5, 7, 19, 22, 49]),
            create_test_posting(100, vec![4, 5, 6, 9, 10]),
        ];

        writer.add_term(1, postings1).unwrap();
        writer.add_term(2, postings2).unwrap();
        writer.finish().unwrap();

        // Read back and verify term 1
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata = writer.get_term_metadata(1).unwrap();
        let mut block = Block::new(metadata.block_ids[0], Some(64));
        block.decode(&mut reader).unwrap();

        let mut chunks = block.decode_chunks_for_term(1, 0, CompressionAlgorithm::VarByte);
        chunks[0].decode_doc_ids();
        chunks[0].decode_doc_frequencies();
        assert_eq!(chunks[0].doc_ids.len(), 4);

        let posting1 = chunks[0].get_posting_list(0);
        assert_eq!(posting1, vec![1, 6, 7, 13, 20]);

        let posting2 = chunks[0].get_posting_list(1);
        assert_eq!(posting2, vec![2, 6, 8, 9]);

        let posting3 = chunks[0].get_posting_list(2);
        assert_eq!(posting3, vec![3, 5]);

        let posting4 = chunks[0].get_posting_list(3);
        assert_eq!(posting4, vec![4, 5, 6, 9, 10]);

        // Verify term 2
        let mut chunks2 = block.decode_chunks_for_term(2, 1, CompressionAlgorithm::VarByte);
        chunks2[0].decode_doc_frequencies();
        let posting1 = chunks2[0].get_posting_list(0);
        assert_eq!(posting1, vec![1, 6, 7, 13, 20]);

        let posting2 = chunks2[0].get_posting_list(1);
        assert_eq!(posting2, vec![2, 6, 8, 9]);

        let posting3 = chunks2[0].get_posting_list(2);
        assert_eq!(posting3, vec![3, 5, 7, 19, 22, 49]);

        let posting4 = chunks2[0].get_posting_list(3);
        assert_eq!(posting4, vec![4, 5, 6, 9, 10]);
    }

    #[test]
    fn test_multiple_blocks_same_term_verify_all_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(128),
            Some(32),
            true,
            CompressionAlgorithm::VarByte,
        );

        // Create postings
        let mut postings = Vec::new();
        for i in 0..200 {
            postings.push(create_test_posting((i + 1) * 100, vec![1, 2, 3, 4, 5]));
        }

        writer.add_term(1, postings.clone()).unwrap();
        writer.finish().unwrap();

        // Read back all postings from all blocks
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata = writer.get_term_metadata(1).unwrap();
        let mut postings_read = Vec::new();
        // Read from all blocks
        for block_id in &metadata.block_ids {
            let mut block = Block::new(*block_id, Some(32));
            block.decode(&mut reader).unwrap();

            let term_index = block.check_if_term_exists(1);
            assert!(term_index >= 0);

            let mut chunks =
                block.decode_chunks_for_term(1, term_index as usize, CompressionAlgorithm::VarByte);
            for chunk in &mut chunks {
                chunk.decode_doc_ids();
                chunk.decode_doc_frequencies();
                for index in 0..chunk.doc_ids.len() {
                    postings_read.push(Posting {
                        doc_id: chunk.doc_ids[index],
                        positions: chunk.get_posting_list(index),
                    });
                }
            }
        }

        // Verify all postings were read correctly
        assert_eq!(postings_read.len(), 200);
        assert_eq!(postings, postings_read);
    }

    #[test]
    fn test_term_without_positions() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(64),
            Some(3),
            false, // Don't include positions
            CompressionAlgorithm::VarByte,
        );

        let postings = vec![
            create_test_posting(10, vec![5, 10, 15]),
            create_test_posting(20, vec![3, 7]),
        ];

        writer.add_term(1, postings).unwrap();
        writer.finish().unwrap();

        // Read back and verify
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata = writer.get_term_metadata(1).unwrap();
        let mut block = Block::new(metadata.block_ids[0], Some(3));
        block.decode(&mut reader).unwrap();

        let chunks = block.decode_chunks_for_term(1, 0, CompressionAlgorithm::VarByte);
        assert_eq!(chunks.len(), 1);

        // Positions should be empty when include_positions is false
        let posting1 = chunks[0].get_posting_list(0);
        assert!(posting1.is_empty());
    }

    #[test]
    fn test_get_term_metadata_nonexistent() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, Some(64), None, true, CompressionAlgorithm::VarByte);

        let result = writer.get_term_metadata(999);
        assert!(result.is_none());
    }

    #[test]
    fn test_large_posting_list() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(128),
            Some(10),
            true,
            CompressionAlgorithm::VarByte,
        );

        // Create a posting with many positions
        let positions: Vec<u32> = (1..=1000).collect();
        let postings = vec![create_test_posting(10, positions.clone())];

        writer.add_term(1, postings).unwrap();
        writer.finish().unwrap();

        // Verify
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata = writer.get_term_metadata(1).unwrap();
        let mut block = Block::new(metadata.block_ids[0], Some(10));
        block.decode(&mut reader).unwrap();

        let mut chunks = block.decode_chunks_for_term(1, 0, CompressionAlgorithm::VarByte);
        chunks[0].decode_doc_frequencies();
        let retrieved_positions = chunks[0].get_posting_list(0);
        assert_eq!(retrieved_positions, positions);
    }

    #[test]
    fn test_multiple_blocks_multiple_terms_verify_all() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(64),
            Some(64),
            false,
            CompressionAlgorithm::VarByte,
        );

        // Create postings for term 1
        let mut postings1 = Vec::new();
        for i in 0..300 {
            postings1.push(create_test_posting((i + 1) * 100, vec![1, 2, 3]));
        }

        // Create postings for term 2
        let mut postings2 = Vec::new();
        for i in 0..180 {
            postings2.push(create_test_posting((i + 1) * 50, vec![4, 5, 6, 7]));
        }

        writer.add_term(1, postings1.clone()).unwrap();
        writer.add_term(2, postings2.clone()).unwrap();
        writer.finish().unwrap();

        // Read back term 1
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata1 = writer.get_term_metadata(1).unwrap();
        let mut postings1_read = Vec::new();

        for block_id in &metadata1.block_ids {
            let mut block = Block::new(*block_id, Some(64));
            block.decode(&mut reader).unwrap();

            let term_index = block.check_if_term_exists(1);
            assert!(term_index >= 0);

            let mut chunks =
                block.decode_chunks_for_term(1, term_index as usize, CompressionAlgorithm::VarByte);

            for chunk in &mut chunks {
                chunk.decode_doc_ids();
                chunk.decode_doc_frequencies();
                for doc_id in &chunk.doc_ids {
                    postings1_read.push(doc_id.clone());
                }
            }
        }

        // Read back term 2
        let metadata2 = writer.get_term_metadata(2).unwrap();
        let mut postings2_read = Vec::new();

        for block_id in &metadata2.block_ids {
            let mut block = Block::new(*block_id, Some(64));
            block.decode(&mut reader).unwrap();

            let term_index = block.check_if_term_exists(2);
            assert!(term_index >= 0);

            let mut chunks =
                block.decode_chunks_for_term(2, term_index as usize, CompressionAlgorithm::VarByte);

            for chunk in &mut chunks {
                chunk.decode_doc_ids();
                chunk.decode_doc_frequencies();
                for doc_id in &chunk.doc_ids {
                    postings2_read.push(doc_id.clone());
                }
            }
        }

        // Verify both terms
        assert_eq!(postings1_read.len(), 300);
        assert_eq!(postings2_read.len(), 180);
    }

    #[test]
    fn test_three_terms_spanning_multiple_blocks() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(128),
            Some(20),
            true,
            CompressionAlgorithm::VarByte,
        );

        // Create postings for three terms with different sizes
        let mut postings1 = Vec::new();
        for i in 0..100 {
            postings1.push(create_test_posting((i + 1) * 10, vec![1, 2]));
        }

        let mut postings2 = Vec::new();
        for i in 0..250 {
            postings2.push(create_test_posting((i + 1) * 20, vec![3, 4, 5]));
        }

        let mut postings3 = Vec::new();
        for i in 0..175 {
            postings3.push(create_test_posting((i + 1) * 15, vec![6, 7, 8, 9]));
        }

        writer.add_term(1, postings1.clone()).unwrap();
        writer.add_term(2, postings2.clone()).unwrap();
        writer.add_term(3, postings3.clone()).unwrap();
        writer.finish().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        // Verify all three terms
        let terms_and_postings = vec![
            (1, postings1, 100),
            (2, postings2, 250),
            (3, postings3, 175),
        ];

        for (term_id, expected_postings, expected_count) in terms_and_postings {
            let metadata = writer.get_term_metadata(term_id).unwrap();
            // assert!(metadata.block_ids.len() > 1);

            let mut postings_read = Vec::new();

            for block_id in &metadata.block_ids {
                let mut block = Block::new(*block_id, Some(20));
                block.decode(&mut reader).unwrap();

                let term_index = block.check_if_term_exists(term_id);
                assert!(term_index >= 0);

                let mut chunks = block.decode_chunks_for_term(
                    term_id,
                    term_index as usize,
                    CompressionAlgorithm::VarByte,
                );

                for chunk in &mut chunks {
                    chunk.decode_doc_ids();
                    chunk.decode_doc_frequencies();
                    for index in 0..chunk.doc_ids.len() {
                        postings_read.push(Posting {
                            doc_id: chunk.doc_ids[index],
                            positions: chunk.get_posting_list(index),
                        });
                    }
                }
            }

            assert_eq!(postings_read.len(), expected_count);
            assert_eq!(expected_postings, postings_read);
        }
    }

    #[test]
    fn test_interleaved_terms_multiple_blocks() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(64),
            Some(64),
            true,
            CompressionAlgorithm::VarByte,
        );

        // Create many terms with moderate posting lists
        let mut all_postings = Vec::new();
        for term_id in 1..=10 {
            let mut postings = Vec::new();
            for i in 0..200 {
                postings.push(create_test_posting(
                    (i + 1) * term_id * 10,
                    vec![1, 2, 3, 4],
                ));
            }
            all_postings.push((term_id, postings));
        }

        // Add all terms
        for (term_id, postings) in &all_postings {
            writer.add_term(*term_id, postings.clone()).unwrap();
        }
        writer.finish().unwrap();

        // Verify all terms
        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        for (term_id, expected_postings) in &all_postings {
            let metadata = writer.get_term_metadata(*term_id).unwrap();

            let mut postings_read = Vec::new();

            for block_id in &metadata.block_ids {
                let mut block = Block::new(*block_id, Some(64));
                block.decode(&mut reader).unwrap();

                let term_index = block.check_if_term_exists(*term_id);
                assert!(term_index >= 0);

                let mut chunks = block.decode_chunks_for_term(
                    *term_id,
                    term_index as usize,
                    CompressionAlgorithm::VarByte,
                );

                for chunk in &mut chunks {
                    chunk.decode_doc_ids();
                    chunk.decode_doc_frequencies();

                    for index in 0..chunk.doc_ids.len() {
                        postings_read.push(Posting {
                            doc_id: chunk.doc_ids[index],
                            positions: chunk.get_posting_list(index),
                        });
                    }
                }
            }

            assert_eq!(postings_read.len(), 200);
            assert_eq!(*expected_postings, postings_read);
        }
    }

    #[test]
    fn test_large_and_small_terms_mixed() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(64),
            Some(10),
            true,
            CompressionAlgorithm::VarByte,
        );

        // Small term (fits in one block)
        let mut small_postings = Vec::new();
        for i in 0..5 {
            small_postings.push(create_test_posting((i + 1) * 10, vec![1]));
        }

        // Large term (spans multiple blocks)
        let mut large_postings = Vec::new();
        for i in 0..300 {
            large_postings.push(create_test_posting((i + 1) * 100, vec![1, 2, 3, 4, 5, 6]));
        }

        // Another small term
        let mut small_postings2 = Vec::new();
        for i in 0..8 {
            small_postings2.push(create_test_posting((i + 1) * 15, vec![2, 3, 45, 122]));
        }

        writer.add_term(1, small_postings.clone()).unwrap();
        writer.add_term(2, large_postings.clone()).unwrap();
        writer.add_term(3, small_postings2.clone()).unwrap();
        writer.finish().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        // Verify small term 1
        let metadata1 = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata1.block_ids.len(), 1);

        // Verify large term 2
        let metadata2 = writer.get_term_metadata(2).unwrap();

        let mut postings2_read = Vec::new();
        for block_id in &metadata2.block_ids {
            let mut block = Block::new(*block_id, Some(10));
            block.decode(&mut reader).unwrap();

            let term_index = block.check_if_term_exists(2);
            assert!(term_index >= 0);

            let mut chunks =
                block.decode_chunks_for_term(2, term_index as usize, CompressionAlgorithm::VarByte);

            for chunk in &mut chunks {
                chunk.decode_doc_ids();
                chunk.decode_doc_frequencies();
                for index in 0..chunk.doc_ids.len() {
                    postings2_read.push(Posting {
                        doc_id: chunk.doc_ids[index],
                        positions: chunk.get_posting_list(index),
                    });
                }
            }
        }

        assert_eq!(postings2_read.len(), 300);
        assert_eq!(large_postings, postings2_read);
    }

    #[test]
    fn test_varying_position_lengths_multiple_blocks() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer = SpimiMergeWriter::new(
            file,
            Some(128),
            Some(10),
            true,
            CompressionAlgorithm::VarByte,
        );

        // Create postings with varying position list lengths
        let mut postings = Vec::new();
        for i in 0..400 {
            let pos_count = (i % 30) + 1;
            let positions: Vec<u32> = (1..=pos_count).collect();
            postings.push(create_test_posting((i + 1) * 100, positions));
        }

        writer.add_term(1, postings.clone()).unwrap();
        writer.finish().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let mut reader = BufReader::new(&mut file);

        let metadata = writer.get_term_metadata(1).unwrap();

        let mut postings_read = Vec::new();

        for block_id in &metadata.block_ids {
            let mut block = Block::new(*block_id, Some(10));
            block.decode(&mut reader).unwrap();

            let term_index = block.check_if_term_exists(1);
            assert!(term_index >= 0);

            let mut chunks =
                block.decode_chunks_for_term(1, term_index as usize, CompressionAlgorithm::VarByte);

            for chunk in &mut chunks {
                chunk.decode_doc_ids();
                chunk.decode_doc_frequencies();
                for index in 0..chunk.doc_ids.len() {
                    postings_read.push(Posting {
                        doc_id: chunk.doc_ids[index],
                        positions: chunk.get_posting_list(index),
                    });
                }
            }
        }

        // chunks are all written properly and chunk size are correct
        assert_eq!(postings_read.len(), 400);
        assert_eq!(postings.len(), postings_read.len());
        for i in 0..postings.len() {
            assert_eq!(postings[i], postings_read[i])
        }
    }
}

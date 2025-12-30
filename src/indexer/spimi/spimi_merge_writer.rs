use std::{
    fs::File,
    io::{self, BufWriter, Write},
};

use crate::{
    compressor::compressor::CompressionAlgorithm,
    utils::{
        block::{Block, MINIMUM_BLOCK_SIZE},
        chunk::Chunk,
        posting::Posting,
    },
};

// An inverted list in the index will often stretch across multiple blocks, starting somewhere in one block and ending some-
// where in another block. Blocks are the basic unit for fetching index data from disk, and for caching index data in main memory.
// Each block contains a large number of postings from one or more inverted lists. These postings are again divided into chunks.
// For example, we may divide the postings of an inverted list into chunks with at max 128 postings each.
// A block then consists of some metadata at the beginning, with information about how many inverted lists are in
// this block and where they start. Chunks are our basic unit for decompressing inverted index data,
// and decompression code is tuned to decompress a chunk in fractions of a microsecond.
// (In fact, this index organization allows us to first decode all docIDs of a chunk, and then later the
// frequencies or positions if needed.)

pub struct SpimiMergeWriter {
    buffered_block_bytes: Vec<u8>, // the buffered block bytes which are written to the disk at intervals
    // pub term_metadata: HashMap<u32, TermMetadata>,
    pub current_block_no: u32,   // the id or number of the current block
    pub current_block: Block,    // the current block which is being written to
    pub include_positions: bool, // whether or not positions should be included in our chunks
    file_writer: BufWriter<File>,
    compression_algorithm: CompressionAlgorithm, // the compression algorithm which is going to be used for the chunks
    pub chunk_size: u8,                          // maximum number of postings in a single chunk
    block_buffer: Vec<u8>,
}

impl SpimiMergeWriter {
    pub fn new(
        file: File,
        chunk_size: Option<u8>,
        block_size: Option<u8>,
        include_positions: bool,
        compression_algorithm: CompressionAlgorithm,
    ) -> Self {
        Self {
            buffered_block_bytes: Vec::with_capacity(3_000_000),
            // term_metadata: HashMap::new(),
            current_block_no: 0,
            current_block: Block::new(0, block_size),
            include_positions,
            block_buffer: vec![0; 64000],
            file_writer: BufWriter::new(file),
            compression_algorithm,
            chunk_size: chunk_size.unwrap_or(128),
        }
    }

    pub fn finish(&mut self) -> io::Result<()> {
        self.current_block.encode(&mut self.block_buffer);
        self.buffered_block_bytes.append(&mut self.block_buffer);
        self.file_writer.write_all(&self.buffered_block_bytes)?;
        self.flush()?;
        self.current_block_no += 1;
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.finish()
    }

    pub fn add_term(&mut self, term: u32, postings: Vec<Posting>) -> io::Result<Vec<u32>> {
        // if it is not possible to add a new term to the block then we will reset the block
        // and write it to the index file
        // the minimum number of bytes necessary to add a new term is 6 bytes for term and term_offset
        if self.current_block.space_left() <= MINIMUM_BLOCK_SIZE {
            self.write_block_to_index_file()?;
            self.current_block.reset();
            self.current_block.set_block_id(self.current_block_no);
        }

        let mut block_ids: Vec<u32> = Vec::new();
        let mut current_chunk = Chunk::new(term, self.compression_algorithm.clone());

        // the term metadata has to be initialized and the current block no has to be added to the
        //  metadata
        // self.initialize_term_metadata(term);
        // self.add_block_to_term_metadata(term, self.current_block_no);
        // self.add_frequency_to_term_metadata(term, postings.len() as u32);

        // we add the term to the block
        self.current_block.add_term(term);
        block_ids.push(self.current_block_no);
        let mut i = 0;
        let postings_length = postings.len();

        let mut postings_iter = postings.into_iter();
        loop {
            // Once the chunk is full, it is encoded and added to the block
            if current_chunk.no_of_postings >= self.chunk_size {
                let chunk_bytes = current_chunk.encode();

                // we check to see if this chunk can be added to the current block
                // if that is not possible we write the current block and we start a new block
                if self.current_block.space_left() >= chunk_bytes.len() as u32 {
                    self.current_block.add_chunk_bytes(chunk_bytes);
                } else {
                    self.write_block_to_index_file()?;

                    self.current_block.reset();
                    self.current_block.set_block_id(self.current_block_no);
                    self.current_block.add_term(term);

                    block_ids.push(self.current_block_no);
                    if chunk_bytes.len() as u32 > self.current_block.space_left() {
                        panic!("chunk cannot fit in block")
                    }
                    self.current_block.add_chunk_bytes(chunk_bytes);
                }

                if i == postings_length {
                    block_ids.shrink_to_fit();
                    return Ok(block_ids);
                }

                current_chunk.reset();
            }

            // We have reached the end of this posting list
            let current_posting = match postings_iter.next() {
                Some(p) => p,
                None => {
                    let chunk_bytes = current_chunk.encode();
                    if self.current_block.space_left() >= chunk_bytes.len() as u32 {
                        self.current_block.add_chunk_bytes(chunk_bytes);
                    } else {
                        self.write_block_to_index_file()?;

                        self.current_block.reset();
                        self.current_block.set_block_id(self.current_block_no);
                        self.current_block.add_term(term);

                        block_ids.push(self.current_block_no);
                        if chunk_bytes.len() as u32 > self.current_block.space_left() {
                            panic!("chunk cannot fit in block")
                        }
                        self.current_block.add_chunk_bytes(chunk_bytes);
                    }
                    block_ids.shrink_to_fit();
                    return Ok(block_ids);
                }
            };

            // we add this doc to the current chunk
            current_chunk.add_doc_id(current_posting.doc_id);
            current_chunk.add_doc_frequency(current_posting.positions.len() as u32);
            if current_posting.positions.len() > 0 && self.include_positions {
                current_chunk.add_doc_positions(current_posting.positions);
            }
            i += 1;
        }
    }

    fn write_block_to_index_file(&mut self) -> io::Result<()> {
        self.current_block.encode(&mut self.block_buffer);
        self.buffered_block_bytes.append(&mut self.block_buffer);
        if self.buffered_block_bytes.len() >= 3_000_000 {
            self.file_writer.write_all(&self.buffered_block_bytes)?;
            self.flush()?;
            self.buffered_block_bytes.clear();
        }
        self.current_block_no += 1;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file_writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    use tempfile::NamedTempFile;

    // Helper function to create test postings
    fn create_test_postings(doc_id: u32, positions: Vec<u32>) -> Posting {
        Posting { doc_id, positions }
    }

    #[test]
    fn test_new_writer() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let writer = SpimiMergeWriter::new(file, None, None, true, CompressionAlgorithm::Simple16);

        assert_eq!(writer.current_block_no, 0);
        assert_eq!(writer.chunk_size, 128);
    }

    #[test]
    fn test_new_writer_with_custom_block_size() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let writer =
            SpimiMergeWriter::new(file, Some(64), None, true, CompressionAlgorithm::Simple16);

        assert_eq!(writer.chunk_size, 64);
    }

    #[test]
    fn test_add_single_term_small_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, Some(64), None, true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(10, vec![5, 10, 15]),
            create_test_postings(20, vec![3, 7]),
        ];

        let result = writer.add_term(1, postings);
        writer.finish().unwrap();
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_multiple_terms() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, Some(64), None, true, CompressionAlgorithm::Simple16);

        let postings1 = vec![create_test_postings(10, vec![1])];
        let postings2 = vec![create_test_postings(20, vec![2])];

        writer.add_term(1, postings1).unwrap();
        writer.add_term(2, postings2).unwrap();
        writer.finish().unwrap();

        // {
        //     let metadata1 = writer.get_term_metadata(1).unwrap();
        //     assert_eq!(metadata1.term_frequency, 1);
        //     assert!(metadata1.block_ids.len() > 0);
        // } // metadata1 reference dropped here

        // // Check metadata2
        // {
        //     let metadata2 = writer.get_term_metadata(2).unwrap();
        //     assert_eq!(metadata2.term_frequency, 1);
        //     assert!(metadata2.block_ids.len() > 0);
        // }
    }

    #[test]
    fn test_term_with_many_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, Some(64), None, true, CompressionAlgorithm::Simple16);

        // Create 150 postings to test chunk splitting (>128 postings per chunk)
        let mut postings = Vec::new();
        for i in 0..150 {
            postings.push(create_test_postings(i * 10, vec![1, 2]));
        }

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        // let metadata = writer.get_term_metadata(1).unwrap();
        // assert_eq!(metadata.term_frequency, 150);
    }

    #[test]
    fn test_block_size_threshold_triggers_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(1), true, CompressionAlgorithm::Simple16);

        let postings = vec![create_test_postings(10, vec![1, 2, 3, 4, 5])];

        writer.add_term(1, postings.clone()).unwrap();
        let block_no_after_first = writer.current_block_no;

        writer.add_term(2, postings).unwrap();
        writer.finish().unwrap();

        // Second term should trigger a new block due to small max_block_size
        assert!(writer.current_block_no >= block_no_after_first);
    }

    #[test]
    fn test_empty_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        // let metadata = writer.get_term_metadata(1).unwrap();
        // assert_eq!(metadata.term_frequency, 0);
    }

    #[test]
    fn test_postings_with_empty_positions() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(10, vec![]),
            create_test_postings(20, vec![]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        // let metadata = writer.get_term_metadata(1).unwrap();
        // assert_eq!(metadata.term_frequency, 2);
    }

    #[test]
    fn test_postings_with_many_positions() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::Simple16);

        // Create a posting with many positions
        let positions: Vec<u32> = (0..100).map(|i| i * 10).collect();
        let postings = vec![create_test_postings(42, positions)];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        // let metadata = writer.get_term_metadata(1).unwrap();
        // assert_eq!(metadata.term_frequency, 1);
    }

    #[test]
    fn test_file_written_correctly() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(10, vec![5, 10]),
            create_test_postings(20, vec![3]),
        ];

        writer.add_term(1, postings).unwrap();
        writer.finish().unwrap();
        // Reopen file and check it has content
        let mut file = temp_file.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();

        // File should contain data
        assert!(buffer.len() > 0);

        // First 4 bytes should be number of terms (at least 1)
        let no_of_terms = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        assert!(no_of_terms >= 1);
    }

    #[test]
    fn test_multiple_terms_different_sizes() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::Simple16);

        // Term 1: Few postings
        writer
            .add_term(1, vec![create_test_postings(10, vec![1])])
            .unwrap();

        // Term 2: Many postings
        let many_postings: Vec<Posting> = (0..50)
            .map(|i| create_test_postings(i * 10, vec![1, 2]))
            .collect();
        writer.add_term(2, many_postings).unwrap();

        // Term 3: Postings with many positions
        writer
            .add_term(3, vec![create_test_postings(100, (0..50).collect())])
            .unwrap();

        // Term 4: Empty
        writer.add_term(4, vec![]).unwrap();

        // Term 5: Normal
        writer
            .add_term(
                5,
                vec![
                    create_test_postings(200, vec![1, 2, 3]),
                    create_test_postings(300, vec![4, 5, 6]),
                ],
            )
            .unwrap();
        writer.finish().unwrap();

        // assert_eq!(writer.get_term_metadata(1).unwrap().term_frequency, 1);
        // assert_eq!(writer.get_term_metadata(2).unwrap().term_frequency, 50);
        // assert_eq!(writer.get_term_metadata(3).unwrap().term_frequency, 1);
        // assert_eq!(writer.get_term_metadata(4).unwrap().term_frequency, 0);
        // assert_eq!(writer.get_term_metadata(5).unwrap().term_frequency, 2);
    }

    #[test]
    fn test_large_doc_ids() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(64), true, CompressionAlgorithm::VarByte);

        let postings = vec![
            create_test_postings(u32::MAX - 1000, vec![1]),
            create_test_postings(u32::MAX - 500, vec![2]),
            create_test_postings(u32::MAX - 1, vec![3]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        // let metadata = writer.get_term_metadata(1).unwrap();
        // assert_eq!(metadata.term_frequency, 3);
    }

    #[test]
    fn test_chunk_boundary_128_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(128), true, CompressionAlgorithm::Simple16);

        // Exactly 128 postings - should fit in one chunk
        let postings: Vec<Posting> = (0..128).map(|i| create_test_postings(i, vec![1])).collect();

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        // let metadata = writer.get_term_metadata(1).unwrap();
        // assert_eq!(metadata.term_frequency, 128);
    }

    #[test]
    fn test_chunk_boundary_129_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            SpimiMergeWriter::new(file, None, Some(128), true, CompressionAlgorithm::Simple16);

        // 129 postings - should create multiple chunks
        let postings: Vec<Posting> = (0..129).map(|i| create_test_postings(i, vec![1])).collect();

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        // let metadata = writer.get_term_metadata(1).unwrap();
        // assert_eq!(metadata.term_frequency, 129);
    }
}

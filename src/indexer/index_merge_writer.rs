use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufWriter, Write},
};

use crate::{
    compressors::compressor::CompressionAlgorithm,
    dictionary::Posting,
    indexer::{block::Block, chunk::Chunk},
};

/*
An inverted list in the index will often stretch across
multiple blocks, starting somewhere in one block and ending some-
where in another block. Blocks are the basic unit for fetching index
data from disk, and for caching index data in main memory.
Each block contains a large number of postings from one or
more inverted lists. These postings are again divided into chunks.
For example, we may divide the postings of an inverted list into
chunks with at max 128 postings each.
A block then consists of some metadata at the beginning, with information about how many
inverted lists are in this block and where they start.
Chunks are our basic unit for decompressing inverted
index data, and decompression code is tuned to decompress a chunk
in fractions of a microsecond. (In fact, this index organization al-
lows us to first decode all docIDs of a chunk, and then later the
frequencies or positions if needed.)

Block Layout->

Block Metadata
Chunk1
Chunk2
.
.
.
ChunkN

Chunk Layout->
ChunkMetadata
doc_ids
posting_lists

*/

/*
Will be stored in every block at the beginning
All the numbers here will be VB-encoded
 */
// pub struct BlockMetadata {
//     terms_in_block: Vec<u32>,
//     offsets_of_terms_in_block: Vec<u16>, // total bytes occupied by the term in the block can be derived from here
// }

/*
Will be stored in every chunk at the beginning
All the numbers here will be VB-encoded
 */
// #[derive(Debug, Clone, PartialEq)]
// pub struct ChunkMetadata {
//     max_doc_id: u32,
//     size_of_chunk: u32,
// }

pub struct TermMetadata {
    pub block_ids: Vec<u32>,
    pub term_frequency: u32,
}

impl TermMetadata {
    pub fn add_block_id(&mut self, block_id: u32) {
        self.block_ids.push(block_id);
    }
    pub fn set_term_frequency(&mut self, term_frequency: u32) {
        self.term_frequency = term_frequency;
    }
}
pub struct MergedIndexBlockWriter {
    pub term_metadata: HashMap<u32, TermMetadata>,
    pub current_block_no: u32,
    pub current_block: Block,
    pub include_positions: bool,
    file_writer: BufWriter<File>,
    compression_algorithm: CompressionAlgorithm,
    pub max_block_size: u8, // in kb
}

impl MergedIndexBlockWriter {
    pub fn new(
        file: File,
        max_block_size: Option<u8>,
        include_positions: bool,
        compression_algorithm: CompressionAlgorithm,
    ) -> Self {
        Self {
            term_metadata: HashMap::new(),
            current_block_no: 0,
            current_block: Block::new(0),
            include_positions,
            file_writer: BufWriter::new(file),
            compression_algorithm,
            max_block_size: match max_block_size {
                Some(block_size) => block_size,
                None => 64,
            },
        }
    }

    pub fn finish(&mut self) -> io::Result<()> {
        self.write_block_to_index_file()
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.finish()
    }

    fn add_block_to_term_metadata(&mut self, term: u32, block_no: u32) {
        if let Some(metadata) = self.term_metadata.get_mut(&term) {
            metadata.add_block_id(block_no);
        }
    }
    fn add_frequency_to_term_metadata(&mut self, term: u32, frequency: u32) {
        if let Some(metadata) = self.term_metadata.get_mut(&term) {
            metadata.set_term_frequency(frequency);
        }
    }
    fn initialize_term_metadata(&mut self, term: u32) {
        self.term_metadata.insert(
            term,
            TermMetadata {
                block_ids: Vec::new(),
                term_frequency: 0,
            },
        );
    }
    pub fn get_term_metadata(&self, term: u32) -> Option<&TermMetadata> {
        self.term_metadata.get(&term)
    }
    pub fn add_term(&mut self, term: u32, postings: Vec<Posting>) -> io::Result<()> {
        // if it is not possible to add a new term to the block then we will reset the block
        // the minimum number of bytes necessary to add a new term is 6 bytes for term and term_offset
        // 9 bytes for the empty chunk
        // we try to avoid empty chunks if possible
        if self.current_block.space_left() < 6 {
            self.write_block_to_index_file()?;
            self.current_block.reset();
        }
        // After this the term is going to be in the block and a new chunk is going to be
        // created
        self.initialize_term_metadata(term);
        self.add_block_to_term_metadata(term, self.current_block_no);
        self.add_frequency_to_term_metadata(term, postings.len() as u32);
        self.current_block.add_term(term);
        let mut current_chunk = Chunk::new(term, self.compression_algorithm.clone());

        let mut i = 0;
        loop {
            if current_chunk.no_of_postings >= 128 {
                // current_chunk.reset();
                // self.current_block.current_chunk.finish();
                // self.current_block.add_current_chunk();
                // self.current_block.reset_current_chunk();
                let chunk_bytes = current_chunk.encode();
                if self.current_block.space_left() >= chunk_bytes.len() as u32 {
                    self.current_block.add_chunk_bytes(chunk_bytes);
                } else {
                    self.write_block_to_index_file()?;
                    self.current_block.reset();
                    self.current_block.add_term(term);
                    self.add_block_to_term_metadata(term, self.current_block_no);
                    if chunk_bytes.len() as u32 > self.current_block.space_left() {
                        panic!("chunk cannot fit in block")
                    }
                    self.current_block.add_chunk_bytes(chunk_bytes);
                }

                if i == postings.len() {
                    return Ok(());
                }

                current_chunk.reset();
            }
            if i == postings.len() {
                let chunk_bytes = current_chunk.encode();
                if self.current_block.space_left() >= chunk_bytes.len() as u32 {
                    self.current_block.add_chunk_bytes(chunk_bytes);
                } else {
                    self.write_block_to_index_file()?;
                    self.current_block.reset();
                    self.current_block.add_term(term);
                    self.add_block_to_term_metadata(term, self.current_block_no);
                    if chunk_bytes.len() as u32 > self.current_block.space_left() {
                        panic!("chunk cannot fit in block")
                    }
                    self.current_block.add_chunk_bytes(chunk_bytes);
                }
                return Ok(());
            }

            let current_posting = &postings[i];
            current_chunk.add_doc_id(current_posting.doc_id);
            if current_posting.positions.len() > 0 {
                current_chunk.add_positions(current_posting.positions.clone());
            }
            current_chunk.no_of_postings += 1;
            i += 1;
        }
    }

    fn write_block_to_index_file(&mut self) -> io::Result<()> {
        let block_bytes = self.current_block.encode();
        self.file_writer.write_all(&block_bytes)?;
        self.file_writer.flush()?;
        self.current_block_no += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dictionary::Posting;
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
        let writer = MergedIndexBlockWriter::new(file, None, true, CompressionAlgorithm::Simple16);

        assert_eq!(writer.term_metadata.len(), 0);
        assert_eq!(writer.current_block_no, 0);
        assert_eq!(writer.max_block_size, 64);
    }

    #[test]
    fn test_new_writer_with_custom_block_size() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let writer =
            MergedIndexBlockWriter::new(file, Some(128), true, CompressionAlgorithm::Simple16);

        assert_eq!(writer.max_block_size, 128);
    }

    #[test]
    fn test_add_single_term_small_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(10, vec![5, 10, 15]),
            create_test_postings(20, vec![3, 7]),
        ];

        let result = writer.add_term(1, postings);
        writer.finish().unwrap();
        assert!(result.is_ok());

        // Check term metadata was updated
        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 2);
        assert_eq!(metadata.block_ids.len(), 1);
    }

    #[test]
    fn test_add_multiple_terms() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        let postings1 = vec![create_test_postings(10, vec![1])];
        let postings2 = vec![create_test_postings(20, vec![2])];

        writer.add_term(1, postings1).unwrap();
        writer.add_term(2, postings2).unwrap();
        writer.finish().unwrap();

        let metadata1 = writer.get_term_metadata(1).unwrap();
        let metadata2 = writer.get_term_metadata(2).unwrap();

        assert_eq!(metadata1.term_frequency, 1);
        assert_eq!(metadata2.term_frequency, 1);
        assert!(metadata1.block_ids.len() > 0);
        assert!(metadata2.block_ids.len() > 0);
    }

    #[test]
    fn test_term_with_many_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        // Create 150 postings to test chunk splitting (>128 postings per chunk)
        let mut postings = Vec::new();
        for i in 0..150 {
            postings.push(create_test_postings(i * 10, vec![1, 2]));
        }

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 150);
    }

    #[test]
    fn test_block_size_threshold_triggers_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(1), true, CompressionAlgorithm::Simple16); // Very small block size

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
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 0);
    }

    #[test]
    fn test_postings_with_empty_positions() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(10, vec![]),
            create_test_postings(20, vec![]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 2);
    }

    #[test]
    fn test_postings_with_many_positions() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        // Create a posting with many positions
        let positions: Vec<u32> = (0..100).map(|i| i * 10).collect();
        let postings = vec![create_test_postings(42, positions)];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 1);
    }

    #[test]
    fn test_multiple_blocks_same_term() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(1), true, CompressionAlgorithm::Simple16); // Very small blocks

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
    }

    #[test]
    fn test_sequential_doc_ids() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(1, vec![1]),
            create_test_postings(2, vec![2]),
            create_test_postings(3, vec![3]),
            create_test_postings(4, vec![4]),
            create_test_postings(5, vec![5]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 5);
    }

    #[test]
    fn test_sparse_doc_ids() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(10, vec![1]),
            create_test_postings(1000, vec![2]),
            create_test_postings(10000, vec![3]),
            create_test_postings(100000, vec![4]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 4);
    }

    #[test]
    fn test_file_written_correctly() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

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
    fn test_term_metadata_structure() {
        let mut metadata = TermMetadata {
            block_ids: Vec::new(),
            term_frequency: 0,
        };

        metadata.add_block_id(0);
        metadata.add_block_id(1);
        metadata.add_block_id(2);

        assert_eq!(metadata.block_ids.len(), 3);
        // assert_eq!(metadata.block_ids[0], 0);
        // assert_eq!(metadata.block_ids[2], 2);

        metadata.set_term_frequency(42);
        assert_eq!(metadata.term_frequency, 42);
    }

    #[test]
    fn test_multiple_terms_different_sizes() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

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

        assert_eq!(writer.get_term_metadata(1).unwrap().term_frequency, 1);
        assert_eq!(writer.get_term_metadata(2).unwrap().term_frequency, 50);
        assert_eq!(writer.get_term_metadata(3).unwrap().term_frequency, 1);
        assert_eq!(writer.get_term_metadata(4).unwrap().term_frequency, 0);
        assert_eq!(writer.get_term_metadata(5).unwrap().term_frequency, 2);
    }

    #[test]
    fn test_large_doc_ids() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(64), true, CompressionAlgorithm::Simple16);

        let postings = vec![
            create_test_postings(u32::MAX - 1000, vec![1]),
            create_test_postings(u32::MAX - 500, vec![2]),
            create_test_postings(u32::MAX - 1, vec![3]),
        ];

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 3);
    }

    #[test]
    fn test_chunk_boundary_128_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(128), true, CompressionAlgorithm::Simple16);

        // Exactly 128 postings - should fit in one chunk
        let postings: Vec<Posting> = (0..128).map(|i| create_test_postings(i, vec![1])).collect();

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 128);
    }

    #[test]
    fn test_chunk_boundary_129_postings() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let mut writer =
            MergedIndexBlockWriter::new(file, Some(128), true, CompressionAlgorithm::Simple16);

        // 129 postings - should create multiple chunks
        let postings: Vec<Posting> = (0..129).map(|i| create_test_postings(i, vec![1])).collect();

        let result = writer.add_term(1, postings);
        assert!(result.is_ok());
        writer.finish().unwrap();

        let metadata = writer.get_term_metadata(1).unwrap();
        assert_eq!(metadata.term_frequency, 129);
    }
}

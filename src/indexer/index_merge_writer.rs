use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufWriter, Write},
};

use crate::{dictionary::Posting, indexer::chunk::Chunk};

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
    term_metadata: HashMap<u32, TermMetadata>,
    current_block_no: u32,
    chunks: Vec<Chunk>,
    current_block_size: u32,
    file_writer: BufWriter<File>,
    max_block_size: u8, // in kb
    terms: Vec<u32>,
}

impl MergedIndexBlockWriter {
    pub fn new(file: File, max_block_size: Option<u8>) -> Self {
        Self {
            term_metadata: HashMap::new(),
            current_block_no: 0,
            chunks: Vec::new(),
            current_block_size: 4,
            file_writer: BufWriter::new(file),
            max_block_size: match max_block_size {
                Some(block_size) => block_size,
                None => 64,
            },
            terms: Vec::new(),
        }
    }
    fn reset(&mut self) {
        self.chunks.clear();
        self.current_block_size = 4;
        self.terms.clear();
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
    // fn check_if_block_full(&mut self) -> bool {
    //     self.current_block_size >= (self.max_block_size as u32* 1000).into()
    // }

    fn add_chunk_to_block(&mut self, chunk: Chunk) {
        self.chunks.push(chunk);
    }

    pub fn get_term_metadata(&mut self, term: u32) -> Option<&TermMetadata> {
        self.term_metadata.get(&term)
    }

    pub fn add_term(&mut self, term: u32, postings: Vec<Posting>) -> io::Result<()> {
        if self.current_block_size + 6 + 8 > ((self.max_block_size as u32 * 1000).into()) {
            self.write_block_to_index_file()?;
            self.reset();
        }
        self.terms.push(term);
        self.add_frequency_to_term_metadata(term, postings.len() as u32);
        self.current_block_size += 6;
        let mut i = 0;
        let mut current_chunk = Chunk::new(term);
        self.current_block_size += 8;

        loop {
            if current_chunk.no_of_postings >= 128 {
                // if !current_chunk.is_empty() {
                current_chunk.finish();
                self.add_chunk_to_block(current_chunk.clone());
                // }
                current_chunk.reset();
                self.current_block_size += 8;
            }
            if i == postings.len() {
                // if !current_chunk.is_empty() {
                current_chunk.finish();
                self.add_chunk_to_block(current_chunk.clone());
                self.write_block_to_index_file()?;
                self.reset();
                // }
                return Ok(());
            }

            let current_posting = &postings[i];
            let encoded_doc_id = current_chunk.encode_doc_id(current_posting.doc_id);
            let encoded_positions = current_chunk.encode_positions(&current_posting.positions);
            let size_of_posting = encoded_doc_id.len() as u32 + encoded_positions.len() as u32;
            if self.current_block_size + size_of_posting
                > (self.max_block_size as u32 * 1000).into()
            {
                current_chunk.finish();
                self.add_chunk_to_block(current_chunk.clone());
                self.write_block_to_index_file()?;
                self.reset();
                current_chunk.reset();
                self.current_block_size += 8;
            }
            current_chunk.set_max_doc_id(current_posting.doc_id);
            current_chunk.add_encoded_doc_id(current_posting.doc_id, encoded_doc_id);
            current_chunk.add_encoded_positions(encoded_positions);
            self.current_block_size += size_of_posting;
            current_chunk.no_of_postings += 1;
            i += 1;
        }
    }

    fn write_block_to_index_file(&mut self) -> io::Result<()> {
        let block_no = self.current_block_no;
        let no_of_terms = self.terms.len().to_le_bytes();
        let encoded_terms: Vec<u8> = self.terms.iter().flat_map(|&n| n.to_le_bytes()).collect();
        let mut term_offsets = Vec::new();
        let mut encoded_chunks: Vec<u8> = Vec::new();
        let mut term_offset_start = (6 * self.terms.len()) as u16;
        let mut term_set = HashSet::new();
        for chunk in &self.chunks {
            if !term_set.contains(&chunk.term) {
                term_set.insert(chunk.term);
                term_offsets.extend(term_offset_start.to_le_bytes());
            }
            encoded_chunks.extend(&chunk.encode());
            term_offset_start += (chunk.doc_ids.len() + chunk.positions.len() + 8) as u16;
        }

        for term in term_set {
            self.add_block_to_term_metadata(term, block_no);
        }

        self.file_writer.write_all(&no_of_terms)?;
        self.file_writer.write_all(&encoded_terms)?;
        self.file_writer.write_all(&term_offsets)?;
        self.file_writer.write_all(&encoded_chunks)?;
        self.file_writer.flush()?;
        self.current_block_no += 1;
        Ok(())
    }
}

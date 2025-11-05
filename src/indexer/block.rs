use std::{
    fs::File,
    io::{self, BufReader, Read, Seek},
};

use crate::indexer::chunk::Chunk;

const BLOCK_SIZE: usize = 64000;
pub struct Block {
    pub block_id: u32,
    pub block_bytes: [u8; BLOCK_SIZE],
    pub terms: Vec<u32>,
    pub term_offsets: Vec<u16>,
}

impl Block {
    pub fn new(block_id: u32) -> Self {
        Self {
            block_id: block_id,
            block_bytes: [0; BLOCK_SIZE],
            term_offsets: Vec::new(),
            terms: Vec::new(),
        }
    }

    pub fn check_if_term_exists(&self, term_id: u32) -> i64 {
        if let Ok(index) = self.terms.binary_search(&term_id) {
            return (index as u32).into();
        }
        -1
    }

    pub fn get_chunk_for_term<'a>(& self,term_id: u32,chunks:&'a[Chunk])->&'a Chunk{
        let mut i=0;
        while i<chunks.len(){
            if chunks[i].max_doc_id<term_id{
                i+=1;
            }
        }

        &chunks[i]
    }

    fn decode_chunks_for_term(&self, term_id: u32, term_index: usize) -> Vec<Chunk> {
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
            current_chunk
                .decode(&chunk_bytes[chunk_offset + 4..chunk_offset + chunk_size as usize]);
            chunk_vec.push(current_chunk.clone());
            chunk_offset += chunk_size as usize;
        }
        chunk_vec
    }

    pub fn read_block(&mut self, reader: &mut BufReader<File>) -> io::Result<()> {
        let _ = reader.seek(std::io::SeekFrom::Start(
            (self.block_id * BLOCK_SIZE as u32).into(),
        ))?;
        let _ = reader.read_exact(&mut self.block_bytes)?;
        let no_of_terms_in_block = u32::from_le_bytes(self.block_bytes[0..4].try_into().unwrap());
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

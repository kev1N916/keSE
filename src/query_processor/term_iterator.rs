use std::u32;

use crate::{
    query_processor::utils::BlockMaxIterator,
    scoring::bm_25::{BM25Params, compute_term_score},
    utils::{
        chunk::Chunk, chunk_block_max_metadata::ChunkBlockMaxMetadata,
        chunk_iterator::ChunkIterator,
    },
};

pub struct TermIterator {
    pub term: String,
    pub term_id: u32,
    pub term_frequency: u32,
    pub chunk_iterator: ChunkIterator,
    pub max_score: f32,
    pub block_max_iterator: BlockMaxIterator,
    pub is_complete: bool,
}

impl TermIterator {
    pub fn new(
        term: String,
        term_id: u32,
        term_frequency: u32,
        chunks: Vec<Chunk>,
        max_score: f32,
        chunk_metadata: Vec<ChunkBlockMaxMetadata>,
    ) -> Self {
        Self {
            term,
            term_id,
            term_frequency,
            chunk_iterator: ChunkIterator::new(chunks),
            max_score,
            block_max_iterator: BlockMaxIterator::new(chunk_metadata),
            is_complete: false,
        }
    }

    pub fn get_term(&self) -> &String {
        &self.term
    }

    pub fn get_term_id(&self) -> u32 {
        self.term_id
    }

    pub fn get_no_of_postings(&self) -> u32 {
        self.chunk_iterator.get_no_of_postings()
    }

    pub fn next(&mut self) -> bool {
        let has_next = self.chunk_iterator.next();
        if !has_next {
            self.is_complete = true;
        }
        has_next
    }

    pub fn is_complete(&mut self) -> bool {
        self.is_complete
    }

    pub fn has_next(&mut self) -> bool {
        self.chunk_iterator.has_next()
    }

    pub fn contains_doc_id(&self, doc_id: u32) -> bool {
        self.chunk_iterator.contains_doc_id(doc_id)
    }
    pub fn advance(&mut self, doc_id: u32) {
        self.chunk_iterator.advance(doc_id);
        while self.get_current_doc_id() < doc_id as u64 {
            self.next();
        }
    }
    pub fn get_all_doc_ids(&mut self) -> Vec<u32> {
        let mut doc_ids = Vec::new();
        doc_ids.push(self.get_current_doc_id() as u32);
        while self.next() && !self.is_complete() {
            doc_ids.push(self.get_current_doc_id() as u32);
        }
        doc_ids
    }
    pub fn get_current_doc_id(&self) -> u64 {
        if self.is_complete {
            return u64::MAX;
        }
        self.chunk_iterator.get_doc_id() as u64
    }

    pub fn get_current_doc_frequency(&self) -> u32 {
        self.chunk_iterator.get_doc_frequency()
    }
    pub fn get_current_doc_score(
        &self,
        current_doc_length: &u32,
        avg_doc_length: f32,
        params: &BM25Params,
        n: u32,
    ) -> f32 {
        compute_term_score(
            self.get_current_doc_frequency(),
            *current_doc_length,
            avg_doc_length,
            n,
            self.term_frequency,
            params,
        )
    }

    pub fn get_max_score(&self) -> f32 {
        self.max_score
    }

    pub fn move_block_max_iterator(&mut self, doc_id: u32) {
        self.block_max_iterator.advance(doc_id);
    }

    pub fn get_block_max_score(&mut self) -> f32 {
        self.block_max_iterator.score()
    }

    pub fn get_block_max_last_doc_id(&mut self) -> u32 {
        self.block_max_iterator.last()
    }

    // pub fn set_chunk_metadata(&mut self, chunk_metadata: Vec<ChunkBlockMaxMetadata>) {
    //     self.chunk_metadata = chunk_metadata
    // }
}

use crate::indexer::chunk::{Chunk, ChunkIterator};

pub struct TermIterator {
    pub term: String,
    pub term_id: u32,
    pub chunk_iterator: ChunkIterator,
    pub max_score: f32,
}

impl TermIterator {
    pub fn new(term: String, term_id: u32, chunks: Vec<Chunk>) -> Self {
        Self {
            term,
            chunk_iterator: ChunkIterator::new(chunks),
            term_id,
            max_score: 0.0,
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
        self.chunk_iterator.next()
    }

    pub fn has_next(&mut self) -> bool {
        self.chunk_iterator.has_next()
    }

    pub fn contains_doc_id(&self, doc_id: u32) -> bool {
        self.chunk_iterator.contains_doc_id(doc_id)
    }
    pub fn advance(&mut self, doc_id: u32) -> bool {
        self.chunk_iterator.advance(doc_id)
    }
    pub fn get_all_doc_ids(&mut self) -> Vec<u32> {
        let mut doc_ids = Vec::new();
        doc_ids.push(self.get_current_doc_id());
        while self.next() {
            doc_ids.push(self.get_current_doc_id());
        }
        doc_ids
    }
    pub fn get_current_doc_id(&self) -> u32 {
        self.chunk_iterator.get_doc_id()
    }
    pub fn get_current_doc_score(&self) -> f32 {
        self.chunk_iterator.get_doc_score()
    }
    pub fn get_max_score(&self) -> f32 {
        self.max_score
    }
}

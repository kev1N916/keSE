use crate::utils::chunk::Chunk;

pub struct ChunkIterator {
    pub chunks: Vec<Chunk>,
    pub current_chunk_index: usize,
    pub current_doc_id_index: usize,
    pub decoded_doc_ids: Vec<u32>,
}

impl ChunkIterator {
    pub fn new(chunks: Vec<Chunk>) -> Self {
        Self {
            chunks,
            current_chunk_index: 0,
            current_doc_id_index: 0,
            decoded_doc_ids: Vec::new(),
        }
    }
    pub fn init(&mut self) {
        self.decoded_doc_ids = self.chunks[self.current_chunk_index].get_doc_ids();
        self.current_doc_id_index = 0;
    }
    pub fn get_no_of_postings(&self) -> u32 {
        self.chunks.iter().map(|c| c.no_of_postings as u32).sum()
    }
    pub fn contains_doc_id(&self, doc_id: u32) -> bool {
        self.decoded_doc_ids.contains(&doc_id)
    }

    pub fn advance(&mut self, doc_id: u32) -> bool {
        while self.current_chunk_index + 1 < self.chunks.len()
            && doc_id > self.chunks[self.current_chunk_index].max_doc_id
        {
            self.current_chunk_index += 1;
        }

        if doc_id > self.chunks[self.current_chunk_index].max_doc_id {
            self.init();
            return true;
        }
        return false;
    }

    pub fn next(&mut self) -> bool {
        if self.current_doc_id_index + 1 < self.decoded_doc_ids.len() {
            self.current_doc_id_index += 1;
            return true;
        } else {
            if self.current_chunk_index + 1 < self.chunks.len() {
                self.current_chunk_index += 1;
                self.init();
                return true;
            } else {
                return false;
            }
        }
    }

    pub fn has_next(&mut self) -> bool {
        if self.current_doc_id_index + 1 < self.decoded_doc_ids.len() {
            return true;
        } else {
            if self.current_chunk_index + 1 < self.chunks.len() {
                return true;
            } else {
                return false;
            }
        }
    }

    pub fn get_doc_id(&self) -> u32 {
        self.decoded_doc_ids[self.current_doc_id_index]
    }
    pub fn get_doc_score(&self) -> f32 {
        0.0
        // self.decoded_doc_ids[self.current_doc_id_index]
    }
    pub fn get_posting_list(&self) -> Vec<u32> {
        self.chunks[self.current_chunk_index].get_posting_list(self.current_doc_id_index)
    }
}

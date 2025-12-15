use crate::{
    in_memory_index::in_memory_index_metadata::InMemoryIndexMetadata,
    my_bk_tree::{self, BkTree},
    utils::{
        chunk_block_max_metadata::ChunkBlockMaxMetadata,
        in_memory_term_metadata::InMemoryTermMetadata,
    },
};

pub struct InMemoryIndex {
    pub no_of_docs: u32,  // no of documents in the collection
    pub no_of_terms: u32, // no of terms in the collection
    pub bk_tree: BkTree,
    pub in_memory_index_metadata: InMemoryIndexMetadata,
}

impl InMemoryIndex {
    pub fn new() -> Self {
        Self {
            no_of_docs: 0,
            no_of_terms: 0,
            bk_tree: my_bk_tree::BkTree::new(),
            in_memory_index_metadata: InMemoryIndexMetadata::new(),
        }
    }

    pub fn get_term_metadata(&self, term: &str) -> &InMemoryTermMetadata {
        self.in_memory_index_metadata.get_term_metadata(term)
    }

    pub fn get_all_terms(&self) -> Vec<String> {
        self.in_memory_index_metadata.get_terms()
    }

    pub fn get_term_id(&self, term: String) -> u32 {
        self.in_memory_index_metadata.get_term_id(term)
    }

    // pub fn add_term(&mut self,term:String,term_id:u32,block_ids:Vec<u32>,term_frequency:u32){
    //     self.bk_tree.add(&term);
    //     self.in_memory_dict.add_term(&term, block_ids, term_frequency, term_id);
    // }

    pub fn add_term_to_bk_tree(&mut self, term: String) {
        self.bk_tree.add(&term);
    }

    pub fn set_term_id(&mut self, term: &str, term_id: u32) {
        self.in_memory_index_metadata.set_term_id(term, term_id);
    }

    pub fn set_chunk_block_max_metadata(
        &mut self,
        term: &str,
        chunk_block_max_metadata: Vec<ChunkBlockMaxMetadata>,
    ) {
        self.in_memory_index_metadata
            .set_chunk_block_max_metadata(term, chunk_block_max_metadata);
    }

    pub fn set_term_frequency(&mut self, term: &str, term_frequency: u32) {
        self.in_memory_index_metadata
            .set_term_frequency(term, term_frequency);
    }

    pub fn set_max_term_score(&mut self, term: &str, max_term_score: f32) {
        self.in_memory_index_metadata
            .set_max_term_score(term, max_term_score);
    }

    pub fn set_block_ids(&mut self, term: &str, block_ids: Vec<u32>) {
        self.in_memory_index_metadata.set_block_ids(term, block_ids);
    }
}

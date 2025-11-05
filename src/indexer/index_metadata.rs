use crate::{in_memory_dict::map_in_memory_dict::MapInMemoryDict, my_bk_tree::{self, BkTree}};

pub struct InMemoryIndexMetatdata {
    pub bk_tree: BkTree,
    pub in_memory_dict: MapInMemoryDict,
}

impl InMemoryIndexMetatdata {
    pub fn new() -> Self {
        Self {
            bk_tree: my_bk_tree::BkTree::new(),
            in_memory_dict: MapInMemoryDict::new(),
        }
    }

    pub fn get_all_terms(&self)->Vec<String>{
        self.in_memory_dict.get_terms()
    }

    pub fn get_term_id(&self,term:String)->u32{
        self.in_memory_dict.get_term_id(term)
    }

    // pub fn add_term(&mut self,term:String,term_id:u32,block_ids:Vec<u32>,term_frequency:u32){
    //     self.bk_tree.add(&term);
    //     self.in_memory_dict.add_term(&term, block_ids, term_frequency, term_id);
    // }

      pub fn add_term_to_bk_tree(&mut self,term:String){
        self.bk_tree.add(&term);
    }

    pub fn set_term_id(&mut self,term:&str,term_id:u32){
        self.in_memory_dict.set_term_id(term, term_id);
    }

    pub fn set_term_frequency(&mut self,term:&str,term_frequency:u32){
        self.in_memory_dict.set_term_frequency(term, term_frequency);
    }

    pub fn set_block_ids(&mut self,term:&str,block_ids:Vec<u32>){
        self.in_memory_dict.set_block_ids(term, block_ids);
    }
}

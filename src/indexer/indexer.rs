use std::collections::HashMap;

use crate::{in_memory_dict::map_in_memory_dict::MapInMemoryDict, my_bk_tree::BkTree};

pub struct IndexMetadata {
    bk_tree: BkTree,
    in_memory_dictionary: MapInMemoryDict,
    term_to_id_map: HashMap<String, u32>,
}

impl IndexMetadata {
    pub fn new() -> Self {
        Self {
            bk_tree: BkTree::new(),
            in_memory_dictionary: MapInMemoryDict::new(),
            term_to_id_map: HashMap::new(),
        }
    }
    pub fn add_term(term: String) {}
}

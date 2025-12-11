use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct MapInMemoryDictPointer {
    pub term_id: u32,
    pub term_frequency: u32,
    pub max_score: f32,
    pub block_ids: Vec<u32>,
}

impl MapInMemoryDictPointer {
    pub fn new(term_id: u32) -> Self {
        Self {
            term_id,
            term_frequency: 0,
            max_score: 0.0,
            block_ids: Vec::new(),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MapInMemoryDict {
    term_map: HashMap<String, MapInMemoryDictPointer>,
}

impl MapInMemoryDict {
    pub fn new() -> MapInMemoryDict {
        return MapInMemoryDict {
            term_map: HashMap::new(),
        };
    }

    pub fn get_terms(&self) -> Vec<String> {
        let mut keys = Vec::new();
        for (key, _) in &self.term_map {
            keys.push(key.to_string());
        }
        keys
    }

    pub fn get_term_id(&self, term: String) -> u32 {
        if let Some(pointer) = self.term_map.get(&term) {
            pointer.term_id
        } else {
            0
        }
    }

    pub fn get_term_metadata(&self, term: &str) -> &MapInMemoryDictPointer {
        self.term_map.get(term).unwrap()
    }

    pub fn set_term_id(&mut self, term: &str, term_id: u32) {
        self.term_map
            .insert(term.to_string(), MapInMemoryDictPointer::new(term_id));
    }

    pub fn set_term_frequency(&mut self, term: &str, term_frequency: u32) {
        if let Some(pointer) = self.term_map.get_mut(term) {
            pointer.term_frequency = term_frequency;
        }
    }

    pub fn set_max_term_score(&mut self, term: &str, max_term_score: f32) {
        if let Some(pointer) = self.term_map.get_mut(term) {
            pointer.max_score = max_term_score;
        }
    }

    pub fn set_block_ids(&mut self, term: &str, block_ids: Vec<u32>) {
        if let Some(pointer) = self.term_map.get_mut(term) {
            pointer.block_ids = block_ids;
        }
    }

    pub fn find(&mut self, term: &str) -> Option<&MapInMemoryDictPointer> {
        self.term_map.get(term)
    }
}

use std::collections::HashMap;

use crate::utils::{
    chunk_block_max_metadata::ChunkBlockMaxMetadata, in_memory_term_metadata::InMemoryTermMetadata,
};

#[derive(Debug, Clone, PartialEq)]
pub struct InMemoryIndexMetadata {
    term_frequencies: Vec<u32>,
    term_block_ids: Vec<u32>,
    term_offsets: Vec<usize>,
    term_max_scores: Vec<f32>,
    term_to_id_map: HashMap<String, u32>,
    term_block_max_metadata: Vec<Box<[ChunkBlockMaxMetadata]>>,
}

impl InMemoryIndexMetadata {
    pub fn new() -> InMemoryIndexMetadata {
        return InMemoryIndexMetadata {
            term_to_id_map: HashMap::with_capacity(6_000_000),
            term_frequencies: Vec::with_capacity(6_000_000),
            term_max_scores: Vec::with_capacity(6_000_000),
            term_block_ids: Vec::with_capacity(6_000_000),
            term_offsets: Vec::with_capacity(6_000_000),
            term_block_max_metadata: Vec::with_capacity(6_000_000),
        };
    }

    // pub fn encode(&self) -> Vec<u8> {
    //     let mut buffer = Vec::new();

    //     // Write number of entries in the HashMap (4 bytes)
    //     buffer.extend_from_slice(&(self.term_metadata.len() as u32).to_le_bytes());

    //     // Write each term and its metadata
    //     for (term, metadata) in &self.term_metadata {
    //         // Write term length (4 bytes)
    //         let term_bytes = term.as_bytes();
    //         buffer.extend_from_slice(&(term_bytes.len() as u32).to_le_bytes());

    //         // Write term bytes
    //         buffer.extend_from_slice(term_bytes);

    //         // Write metadata
    //         let metadata_bytes = metadata.encode();

    //         // Write metadata length (4 bytes)
    //         buffer.extend_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());

    //         // Write metadata bytes
    //         buffer.extend_from_slice(&metadata_bytes);
    //     }

    //     buffer
    // }

    // pub fn decode(bytes: &[u8]) -> Result<Self, String> {
    //     use std::io::{Cursor, Read};

    //     let mut cursor = Cursor::new(bytes);
    //     let mut buf = [0u8; 4];

    //     // Read number of entries (4 bytes)
    //     cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
    //     let num_entries = u32::from_le_bytes(buf) as usize;

    //     // Pre-allocate HashMap
    //     let mut term_metadata = HashMap::with_capacity(num_entries);

    //     // Read each term and its metadata
    //     for _ in 0..num_entries {
    //         // Read term length (4 bytes)
    //         cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
    //         let term_len = u32::from_le_bytes(buf) as usize;

    //         // Read term bytes
    //         let mut term_bytes = vec![0u8; term_len];
    //         cursor
    //             .read_exact(&mut term_bytes)
    //             .map_err(|e| e.to_string())?;
    //         let term = String::from_utf8(term_bytes).map_err(|e| e.to_string())?;

    //         // Read metadata length (4 bytes)
    //         cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
    //         let metadata_len = u32::from_le_bytes(buf) as usize;

    //         // Read metadata bytes
    //         let mut metadata_bytes = vec![0u8; metadata_len];
    //         cursor
    //             .read_exact(&mut metadata_bytes)
    //             .map_err(|e| e.to_string())?;

    //         // Decode metadata
    //         let metadata = InMemoryTermMetadata::decode(&metadata_bytes)?;

    //         // Insert into HashMap
    //         term_metadata.insert(term, metadata);
    //     }

    //     Ok(Self { term_metadata })
    // }

    pub fn get_terms(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(self.term_to_id_map.len());
        for (key, _) in &self.term_to_id_map {
            keys.push(key.as_str());
        }
        keys
    }

    pub fn get_term_metadata<'a>(&'a self, term: &str) -> Option<InMemoryTermMetadata<'a>> {
        let term_id = self.get_term_id(term);
        if term_id == 0 {
            return None;
        }

        Some(InMemoryTermMetadata {
            term_id: self.get_term_id(term),
            term_frequency: self.get_term_frequency(term_id),
            max_score: self.get_max_term_score(term_id),
            block_ids: self.get_block_ids(term_id),
            chunk_block_max_metadata: self.get_chunk_block_max_metadata(term_id),
        })
    }

    pub fn set_term_id(&mut self, term: String, term_id: u32) {
        self.term_to_id_map.insert(term, term_id);
    }

    pub fn get_term_id(&self, term: &str) -> u32 {
        if let Some(pointer) = self.term_to_id_map.get(term) {
            *pointer
        } else {
            0
        }
    }

    pub fn set_term_frequency(&mut self, term_frequency: u32) {
        self.term_frequencies.push(term_frequency)
    }

    pub fn get_term_frequency(&self, term_id: u32) -> u32 {
        self.term_frequencies[(term_id - 1) as usize]
    }

    pub fn set_max_term_score(&mut self, max_term_score: f32) {
        self.term_max_scores.push(max_term_score)
    }

    pub fn get_max_term_score(&self, term_id: u32) -> f32 {
        self.term_max_scores[(term_id - 1) as usize]
    }

    pub fn set_chunk_block_max_metadata(
        &mut self,
        chunk_block_max_metadata: Vec<ChunkBlockMaxMetadata>,
    ) {
        let boxed: Box<[ChunkBlockMaxMetadata]> = chunk_block_max_metadata.into();
        self.term_block_max_metadata.push(boxed);
    }

    pub fn get_chunk_block_max_metadata(
        &self,
        term_id: u32,
    ) -> Option<&Box<[ChunkBlockMaxMetadata]>> {
        if self.term_block_max_metadata.len() > (term_id - 1) as usize {
            return Some(&self.term_block_max_metadata[(term_id - 1) as usize]);
        }
        None
    }

    pub fn set_block_ids(&mut self, block_ids: Vec<u32>) {
        self.term_offsets.push(self.term_block_ids.len());
        self.term_block_ids.extend(block_ids);
    }

    pub fn get_block_ids(&self, term_id: u32) -> &[u32] {
        let term_id = term_id as usize;

        let term_offset_start = self.term_offsets[term_id - 1];
        let term_offset_end = match term_id > self.term_offsets.len() - 1 {
            false => self.term_offsets[term_id],
            true => self.term_block_ids.len(),
        };

        &self.term_block_ids[term_offset_start..term_offset_end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;

    #[test]
    fn test_new_creates_empty_metadata() {
        let metadata = InMemoryIndexMetadata::new();
        assert_eq!(metadata.get_terms().len(), 0);
    }

    #[test]
    fn test_set_and_get_term_id() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("hello".to_string(), 42);

        assert_eq!(metadata.get_term_id("hello"), 42);
    }

    #[test]
    fn test_get_term_id_returns_zero_for_nonexistent_term() {
        let metadata = InMemoryIndexMetadata::new();
        assert_eq!(metadata.get_term_id("nonexistent"), 0);
    }

    #[test]
    fn test_get_terms_returns_all_terms() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("apple".to_string(), 1);
        metadata.set_term_id("banana".to_string(), 2);
        metadata.set_term_id("cherry".to_string(), 3);

        let terms = metadata.get_terms();
        assert_eq!(terms.len(), 3);
        assert!(terms.contains(&"apple"));
        assert!(terms.contains(&"banana"));
        assert!(terms.contains(&"cherry"));
    }

    #[test]
    fn test_set_term_frequency() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test".to_string(), 1);
        metadata.set_term_frequency(100);

        let term_frequency = metadata.get_term_frequency(1);
        assert_eq!(term_frequency, 100);
    }

    #[test]
    fn test_set_max_term_score() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test".to_string(), 1);
        metadata.set_max_term_score(0.95);

        let max_score = metadata.get_max_term_score(1);
        assert_eq!(max_score, 0.95);
    }

    #[test]
    fn test_set_block_ids() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test".to_string(), 1);
        metadata.set_block_ids(vec![10, 20, 30]);

        let block_ids = metadata.get_block_ids(1);
        assert_eq!(block_ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_set_chunk_block_max_metadata() {
        let mut metadata = InMemoryIndexMetadata::new();
        metadata.set_term_id("test".to_string(), 1);

        let chunks = vec![ChunkBlockMaxMetadata {
            chunk_last_doc_id: 8,
            chunk_max_term_score: 8.67,
        }];
        metadata.set_chunk_block_max_metadata(chunks.clone());

        let chunk_block_max_metadata = metadata.get_chunk_block_max_metadata(1).unwrap();
        assert_eq!(chunk_block_max_metadata.to_vec(), chunks);
    }

    #[test]
    fn test_get_term_metadata_panics_on_nonexistent_term() {
        let metadata = InMemoryIndexMetadata::new();
        assert_eq!(None, metadata.get_term_metadata("nonexistent"));
    }

    #[test]
    fn test_complete_workflow() {
        let mut metadata = InMemoryIndexMetadata::new();

        // Add a term
        metadata.set_term_id("rust".to_string(), 1);
        metadata.set_term_frequency(150);
        metadata.set_max_term_score(0.87);
        metadata.set_block_ids(vec![5, 10, 15]);

        // Verify all properties
        let term_meta = metadata.get_term_metadata("rust").unwrap();
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 150);
        assert_eq!(term_meta.max_score, 0.87);
        assert_eq!(term_meta.block_ids, vec![5, 10, 15]);
    }

    #[test]
    fn test_multiple_terms() {
        let mut metadata = InMemoryIndexMetadata::new();

        metadata.set_term_id("alpha".to_string(), 1);
        metadata.set_term_frequency(10);

        metadata.set_term_id("beta".to_string(), 2);
        metadata.set_term_frequency(20);

        metadata.set_term_id("gamma".to_string(), 3);
        metadata.set_term_frequency(30);

        assert_eq!(metadata.get_term_id("alpha"), 1);
        assert_eq!(metadata.get_term_id("beta"), 2);
        assert_eq!(metadata.get_term_id("gamma"), 3);

        assert_eq!(metadata.get_term_frequency(1), 10);
        assert_eq!(metadata.get_term_frequency(2), 20);
        assert_eq!(metadata.get_term_frequency(3), 30);
    }
}

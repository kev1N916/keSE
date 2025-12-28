use std::{
    collections::HashMap,
    io::{self, Read, Write},
};

use crate::{
    in_memory_index_metadata::bk_tree::BkTree,
    utils::{
        chunk_block_max_metadata::ChunkBlockMaxMetadata,
        in_memory_term_metadata::InMemoryTermMetadata,
    },
};

// While serving queries we will need to know which blocks are occupied by which terms and
// which terms map to which ids so we keep an instance of InMemoryIndexMetadata in memory
// term_max_scores,term_frequencies and term_block_max_metadata are needed for query processing
pub struct InMemoryIndexMetadata {
    pub no_of_blocks: u32,
    pub no_of_docs: u32,  // no of documents in the collection
    pub no_of_terms: u32, // no of terms in the collection
    pub bk_tree: BkTree,
    term_frequencies: Vec<u32>,

    // Vec<Vec<u32>> has been made into a 1D vector
    // we keep track of the indexes in this vector which help
    // us extract the block ids when we need to
    term_block_ids: Vec<u32>,
    term_block_id_offsets: Vec<usize>,

    term_max_scores: Vec<f32>,
    term_to_id_map: HashMap<String, u32>,
    term_block_max_metadata: Vec<Box<[ChunkBlockMaxMetadata]>>,
}

impl InMemoryIndexMetadata {
    pub fn new() -> Self {
        Self {
            no_of_blocks: 0,
            no_of_docs: 0,
            no_of_terms: 0,
            bk_tree: BkTree::new(),
            term_to_id_map: HashMap::with_capacity(6_000_000),
            term_frequencies: Vec::with_capacity(6_000_000),
            term_max_scores: Vec::with_capacity(6_000_000),
            term_block_ids: Vec::with_capacity(6_000_000),
            term_block_id_offsets: Vec::with_capacity(6_000_000),
            term_block_max_metadata: Vec::with_capacity(6_000_000),
        }
    }

    pub fn save_term_metadata<W: Write>(&self, mut writer: W) -> io::Result<()> {
        assert_eq!(
            self.term_block_id_offsets.len(),
            self.term_frequencies.len()
        );
        assert_eq!(self.term_max_scores.len(), self.term_frequencies.len());
        assert_eq!(self.term_to_id_map.len(), self.term_frequencies.len());
        assert_eq!(
            self.term_block_max_metadata.len(),
            self.term_frequencies.len()
        );

        writer.write_all(&self.no_of_blocks.to_le_bytes())?;
        writer.write_all(&self.no_of_docs.to_le_bytes())?;
        writer.write_all(&self.no_of_terms.to_le_bytes())?;

        writer.write_all(&(self.term_frequencies.len() as u32).to_le_bytes())?;
        for i in 0..self.term_frequencies.len() {
            writer.write_all(&self.term_frequencies[i].to_le_bytes())?;
            writer.write_all(&self.term_max_scores[i].to_le_bytes())?;
            writer.write_all(&(self.term_block_id_offsets[i] as u32).to_le_bytes())?;
            // Write term block max metadata
            let metadata = &self.term_block_max_metadata[i];
            writer.write_all(&(metadata.len() as u32).to_le_bytes())?;
            for chunk in metadata.iter() {
                writer.write_all(&chunk.chunk_last_doc_id.to_le_bytes())?;
                writer.write_all(&chunk.chunk_max_term_score.to_le_bytes())?;
            }
        }

        writer.write_all(&(self.term_block_ids.len() as u32).to_le_bytes())?;
        for i in 0..self.term_block_ids.len() {
            writer.write_all(&self.term_block_ids[i].to_le_bytes())?;
        }

        writer.write_all(&(self.term_to_id_map.len() as u32).to_le_bytes())?;
        for (term, id) in &self.term_to_id_map {
            writer.write_all(&(term.len() as u32).to_le_bytes())?;
            writer.write_all(term.as_bytes())?;
            writer.write_all(&id.to_le_bytes())?;
        }
        writer.flush()?;
        Ok(())
    }

    pub fn load_term_metadata<R: Read>(&mut self, mut reader: R) -> io::Result<()> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        self.no_of_blocks = u32::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        self.no_of_docs = u32::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        self.no_of_terms = u32::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let num_terms = u32::from_le_bytes(buf) as usize;

        let mut term_frequencies = Vec::with_capacity(num_terms);
        let mut term_max_scores = Vec::with_capacity(num_terms);
        let mut term_block_id_offsets = Vec::with_capacity(num_terms);
        let mut term_block_max_metadata = Vec::with_capacity(num_terms);

        for _ in 0..num_terms {
            reader.read_exact(&mut buf)?;
            term_frequencies.push(u32::from_le_bytes(buf));
            reader.read_exact(&mut buf)?;
            term_max_scores.push(f32::from_le_bytes(buf));
            reader.read_exact(&mut buf)?;
            let stored_offset = u32::from_le_bytes(buf) as usize;
            term_block_id_offsets.push(stored_offset);

            reader.read_exact(&mut buf)?;
            let num_chunks = u32::from_le_bytes(buf) as usize;
            let mut chunks = Vec::with_capacity(num_chunks);
            for _ in 0..num_chunks {
                reader.read_exact(&mut buf)?;
                let chunk_last_doc_id = u32::from_le_bytes(buf);

                reader.read_exact(&mut buf)?;
                let chunk_max_term_score = f32::from_le_bytes(buf);

                chunks.push(ChunkBlockMaxMetadata {
                    chunk_last_doc_id,
                    chunk_max_term_score,
                });
            }
            term_block_max_metadata.push(chunks.into_boxed_slice());
        }

        self.term_frequencies = term_frequencies;
        self.term_block_id_offsets = term_block_id_offsets;
        self.term_max_scores = term_max_scores;
        self.term_block_max_metadata = term_block_max_metadata;

        reader.read_exact(&mut buf)?;
        let block_id_length = u32::from_le_bytes(buf) as usize;
        let mut term_block_ids = Vec::with_capacity(block_id_length);
        for _ in 0..block_id_length {
            reader.read_exact(&mut buf)?;
            term_block_ids.push(u32::from_le_bytes(buf));
        }

        self.term_block_ids = term_block_ids;

        reader.read_exact(&mut buf)?;
        let map_size = u32::from_le_bytes(buf) as usize;
        let mut term_to_id_map = HashMap::with_capacity(map_size);

        for _ in 0..map_size {
            reader.read_exact(&mut buf)?;
            let term_len = u32::from_le_bytes(buf) as usize;

            let mut term_bytes = vec![0u8; term_len];
            reader.read_exact(&mut term_bytes)?;
            let term = String::from_utf8(term_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            reader.read_exact(&mut buf)?;
            let id = u32::from_le_bytes(buf);

            term_to_id_map.insert(term, id);
        }

        self.term_to_id_map = term_to_id_map;
        assert_eq!(
            self.term_block_id_offsets.len(),
            self.term_frequencies.len()
        );
        assert_eq!(self.term_max_scores.len(), self.term_frequencies.len());
        assert_eq!(self.term_to_id_map.len(), self.term_frequencies.len());
        assert_eq!(
            self.term_block_max_metadata.len(),
            self.term_frequencies.len()
        );
        Ok(())
    }

    // should save memory hopefully
    pub fn close(&mut self) {
        self.term_block_id_offsets.shrink_to_fit();
        self.term_block_max_metadata.shrink_to_fit();
        self.term_frequencies.shrink_to_fit();
        self.term_block_ids.shrink_to_fit();
        self.term_max_scores.shrink_to_fit();
        self.term_to_id_map.shrink_to_fit();
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

    pub fn get_all_terms(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(self.term_to_id_map.len());
        for (key, _) in &self.term_to_id_map {
            keys.push(key.as_str());
        }
        keys
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

    pub fn get_term_frequency(&self, term_id: u32) -> u32 {
        self.term_frequencies[(term_id - 1) as usize]
    }

    pub fn get_max_term_score(&self, term_id: u32) -> f32 {
        self.term_max_scores[(term_id - 1) as usize]
    }

    pub fn add_term_to_bk_tree(&mut self, term: String) {
        self.bk_tree.add(&term);
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

    pub fn set_term_frequency(&mut self, term_frequency: u32) {
        self.term_frequencies.push(term_frequency)
    }

    pub fn set_max_term_score(&mut self, max_term_score: f32) {
        self.term_max_scores.push(max_term_score)
    }

    pub fn set_block_ids(&mut self, block_ids: Vec<u32>) {
        self.term_block_id_offsets.push(self.term_block_ids.len());
        self.term_block_ids.extend(block_ids);
    }

    pub fn get_block_ids(&self, term_id: u32) -> &[u32] {
        let term_id = term_id as usize;

        let term_offset_start = self.term_block_id_offsets[term_id - 1];
        let term_offset_end = match term_id > self.term_block_id_offsets.len() - 1 {
            false => self.term_block_id_offsets[term_id],
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
    fn test_new_creates_empty_index() {
        let index = InMemoryIndexMetadata::new();

        assert_eq!(index.no_of_docs, 0);
        assert_eq!(index.no_of_terms, 0);
        assert_eq!(index.get_all_terms().len(), 0);
    }

    #[test]
    fn test_set_and_get_term_id() {
        let mut in_memory_index_metadata = InMemoryIndexMetadata::new();
        in_memory_index_metadata.set_term_id("hello".to_string(), 42);

        assert_eq!(in_memory_index_metadata.get_term_id("hello"), 42);
    }

    #[test]
    fn test_get_term_id_returns_zero_for_nonexistent_term() {
        let in_memory_index_metadata = InMemoryIndexMetadata::new();
        assert_eq!(in_memory_index_metadata.get_term_id("nonexistent"), 0);
    }

    #[test]
    fn test_get_all_terms_returns_all_terms() {
        let mut in_memory_index_metadata = InMemoryIndexMetadata::new();
        in_memory_index_metadata.set_term_id("apple".to_string(), 1);
        in_memory_index_metadata.set_term_id("banana".to_string(), 2);
        in_memory_index_metadata.set_term_id("cherry".to_string(), 3);

        let terms = in_memory_index_metadata.get_all_terms();
        assert_eq!(terms.len(), 3);
        assert!(terms.contains(&"apple"));
        assert!(terms.contains(&"banana"));
        assert!(terms.contains(&"cherry"));
    }

    #[test]
    fn test_set_term_frequency() {
        let mut in_memory_index_metadata = InMemoryIndexMetadata::new();
        in_memory_index_metadata.set_term_id("test".to_string(), 1);
        in_memory_index_metadata.set_term_frequency(150);

        let term_frequency = in_memory_index_metadata.get_term_frequency(1);
        assert_eq!(term_frequency, 150);
    }

    #[test]
    fn test_complete_workflow() {
        let mut in_memory_index_metadata = InMemoryIndexMetadata::new();

        // Add a term with all metadata
        in_memory_index_metadata.set_term_id("rust".to_string(), 1);
        in_memory_index_metadata.add_term_to_bk_tree("rust".to_string());
        in_memory_index_metadata.set_term_frequency(150);
        in_memory_index_metadata.set_max_term_score(0.87);
        in_memory_index_metadata.set_block_ids(vec![5, 10, 15]);

        // Verify all properties
        let term_meta = in_memory_index_metadata.get_term_metadata("rust").unwrap();
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 150);
        assert_eq!(term_meta.max_score, 0.87);
        assert_eq!(term_meta.block_ids, vec![5, 10, 15]);

        // Verify it's in the terms list
        assert!(in_memory_index_metadata.get_all_terms().contains(&"rust"));
    }

    #[test]
    fn test_multiple_terms_in_index() {
        let mut in_memory_index_metadata = InMemoryIndexMetadata::new();

        // Add multiple terms
        in_memory_index_metadata.set_term_id("alpha".to_string(), 1);
        in_memory_index_metadata.set_term_frequency(10);
        in_memory_index_metadata.add_term_to_bk_tree("alpha".to_string());

        in_memory_index_metadata.set_term_id("beta".to_string(), 2);
        in_memory_index_metadata.set_term_frequency(20);
        in_memory_index_metadata.add_term_to_bk_tree("beta".to_string());

        in_memory_index_metadata.set_term_id("gamma".to_string(), 3);
        in_memory_index_metadata.set_term_frequency(30);
        in_memory_index_metadata.add_term_to_bk_tree("gamma".to_string());

        // Verify term IDs
        assert_eq!(in_memory_index_metadata.get_term_id("alpha"), 1);
        assert_eq!(in_memory_index_metadata.get_term_id("beta"), 2);
        assert_eq!(in_memory_index_metadata.get_term_id("gamma"), 3);

        // Verify term frequencies
        assert_eq!(in_memory_index_metadata.get_term_frequency(1), 10);
        assert_eq!(in_memory_index_metadata.get_term_frequency(2), 20);
        assert_eq!(in_memory_index_metadata.get_term_frequency(3), 30);

        // Verify all terms are present
        let terms = in_memory_index_metadata.get_all_terms();
        assert_eq!(terms.len(), 3);
    }

    #[test]
    fn test_index_with_complex_metadata() {
        let mut in_memory_index_metadata = InMemoryIndexMetadata::new();

        // Add term with comprehensive metadata
        in_memory_index_metadata.set_term_id("comprehensive".to_string(), 1);
        in_memory_index_metadata.add_term_to_bk_tree("comprehensive".to_string());
        in_memory_index_metadata.set_term_frequency(250);
        in_memory_index_metadata.set_max_term_score(0.95);
        in_memory_index_metadata.set_block_ids(vec![1, 5, 10, 15, 20]);

        let chunks: Vec<ChunkBlockMaxMetadata> = vec![ChunkBlockMaxMetadata {
            chunk_last_doc_id: 8,
            chunk_max_term_score: 8.67,
        }];
        in_memory_index_metadata.set_chunk_block_max_metadata(chunks.clone());

        // Verify all metadata
        let term_meta = in_memory_index_metadata
            .get_term_metadata("comprehensive")
            .unwrap();
        assert_eq!(term_meta.term_id, 1);
        assert_eq!(term_meta.term_frequency, 250);
        assert_eq!(term_meta.max_score, 0.95);
        assert_eq!(term_meta.block_ids, vec![1, 5, 10, 15, 20]);
        assert_eq!(term_meta.chunk_block_max_metadata.unwrap().to_vec(), chunks);
    }
}

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader},
    u32,
};

use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_dict::map_in_memory_dict::MapInMemoryDictPointer,
    query_processor::{algos::wand::wand, term_iterator::TermIterator},
    utils::block::Block,
};

pub struct QueryProcessor {
    inverted_index_file: File,
    compression_algorithm: CompressionAlgorithm,
}

impl QueryProcessor {
    pub fn new(compression_algorithm: CompressionAlgorithm) -> io::Result<Self> {
        let inverted_index_file = File::open("final.idx")?;
        Ok(Self {
            inverted_index_file,
            compression_algorithm,
        })
    }

    fn get_doc_ids_for_term(&mut self, block_ids: &[u32], term_id: u32) -> HashSet<u32> {
        let mut reader: BufReader<&mut File> = BufReader::new(&mut self.inverted_index_file);

        let mut doc_ids = HashSet::new();
        for i in 0..block_ids.len() {
            let mut block = Block::new(block_ids[i]);
            block.init(&mut reader).unwrap();
            let term_index = block.check_if_term_exists(term_id);
            let chunks = block.decode_chunks_for_term(
                term_id,
                term_index as usize,
                self.compression_algorithm.clone(),
            );
            for chunk in chunks {
                doc_ids.extend(&mut chunk.get_doc_ids().into_iter());
            }
        }
        doc_ids
    }

    fn intersect(&mut self, block_ids: &[u32], term_id: u32, doc_ids: &mut HashSet<u32>) {
        let mut reader: BufReader<&mut File> = BufReader::new(&mut self.inverted_index_file);
        for i in 0..block_ids.len() {
            let mut block = Block::new(block_ids[i]);
            block.init(&mut reader).unwrap();
            let term_index = block.check_if_term_exists(term_id);
            if term_index == -1 {
                continue;
            }
            let chunks = block.decode_chunks_for_term(
                term_id,
                term_index as usize,
                self.compression_algorithm.clone(),
            );

            doc_ids.retain(|doc_id| {
                if let Some(chunk) = block.get_chunk_for_doc(*doc_id, &chunks) {
                    let chunk_doc_ids = chunk.get_doc_ids();
                    chunk_doc_ids.contains(&doc_id)
                } else {
                    false // Remove if chunk not found
                }
            });
        }
    }

    pub fn process_query(
        &mut self,
        query_terms: Vec<String>,
        query_metadata: Vec<&MapInMemoryDictPointer>,
    ) -> Vec<u32> {
        let mut term_iterators: Vec<TermIterator> = Vec::new();
        let mut block_map: HashMap<u32, Block> = HashMap::new();
        let mut reader: BufReader<&mut File> = BufReader::new(&mut self.inverted_index_file);

        for i in 0..query_metadata.len() {
            let mut chunks = Vec::new();
            for block_id in &query_metadata[i].block_ids {
                let block = block_map.entry(*block_id).or_insert_with(|| {
                    let mut new_block = Block::new(*block_id);
                    let _ = new_block.init(&mut reader);
                    new_block
                });
                let term_index = block.check_if_term_exists(query_metadata[i].term_id);

                if term_index == -1 {
                    continue;
                }

                chunks.extend(block.decode_chunks_for_term(
                    query_metadata[i].term_id,
                    term_index as usize,
                    self.compression_algorithm.clone(),
                ));
            }
            term_iterators.push(TermIterator::new(
                query_terms[i].clone(),
                query_metadata[i].term_id,
                chunks,
                query_metadata[i].max_score,
            ));
        }

        wand(term_iterators)
    }
}

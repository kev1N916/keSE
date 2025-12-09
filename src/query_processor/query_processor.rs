use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader},
    u32,
};

use crate::{
    in_memory_dict::map_in_memory_dict::MapInMemoryDictPointer,
    indexer::{block::Block, indexer::DocumentMetadata},
    query_processor::{algos::wand::wand, term_iterator::TermIterator},
};

pub struct QueryProcessor {
    inverted_index_file: File,
}

impl QueryProcessor {
    pub fn new() -> io::Result<Self> {
        let inverted_index_file = File::open("final.idx")?;
        Ok(Self {
            inverted_index_file,
        })
    }

    fn get_doc_ids_for_term(&mut self, block_ids: &[u32], term_id: u32) -> HashSet<u32> {
        let mut reader: BufReader<&mut File> = BufReader::new(&mut self.inverted_index_file);

        let mut doc_ids = HashSet::new();
        for i in 0..block_ids.len() {
            let mut block = Block::new(block_ids[i]);
            block.init(&mut reader).unwrap();
            let term_index = block.check_if_term_exists(term_id);
            let chunks = block.decode_chunks_for_term(term_id, term_index as usize);
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
            let chunks = block.decode_chunks_for_term(term_id, term_index as usize);

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

    pub fn score_docs(&mut self, doc_metadata: &HashMap<u32, DocumentMetadata>) {}
    // pub fn process_query(
    //     &mut self,
    //     query_terms: Vec<String>,
    //     query_metadata: Vec<&MapInMemoryDictPointer>,
    // ) {
    //     let mut min_frequency_term_index = query_terms.len();
    //     let mut min_doc_frequency = u32::MAX;
    //     for i in 0..query_metadata.len() {
    //         if query_metadata[i].term_frequency < min_doc_frequency {
    //             min_frequency_term_index = i;
    //             min_doc_frequency = query_metadata[i].term_frequency;
    //         }
    //     }

    //     let mut doc_ids = self.get_doc_ids_for_term(
    //         &query_metadata[min_frequency_term_index].block_ids,
    //         query_metadata[min_frequency_term_index].term_id,
    //     );
    //     for i in 0..query_metadata.len() {
    //         if i != min_frequency_term_index {
    //             self.intersect(
    //                 &query_metadata[i].block_ids,
    //                 query_metadata[i].term_id,
    //                 &mut doc_ids,
    //             )
    //         }
    //     }
    // }

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

                chunks.extend(
                    block.decode_chunks_for_term(query_metadata[i].term_id, term_index as usize),
                );
            }
            term_iterators.push(TermIterator::new(
                query_terms[i].clone(),
                query_metadata[i].term_id,
                chunks,
            ));
        }

        wand(term_iterators)
    }
}

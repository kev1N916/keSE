use std::{
    fs::File,
    io::{self, BufReader},
    path::PathBuf,
    u32,
};

use search_engine_cache::CacheType;

use crate::{
    compressor::compressor::CompressionAlgorithm,
    query_processor::{
        retrieval_algorithms::{
            QueryAlgorithm, binary_merge::holistic_binary_merge,
            block_max_max_score::block_max_max_score, block_max_wand::block_max_wand,
            max_score::max_score, wand::wand,
        },
        term_iterator::TermIterator,
    },
    utils::{
        block::Block, in_memory_term_metadata::InMemoryTermMetadata, paths::get_inverted_index_path,
    },
};

pub struct QueryProcessor {
    block_cache: CacheType<u32, Block>,
    inverted_index_file: File,
    compression_algorithm: CompressionAlgorithm,
    query_algorithm: QueryAlgorithm,
}

impl QueryProcessor {
    pub fn new(
        index_directory_path: PathBuf,
        compression_algorithm: CompressionAlgorithm,
        query_algorithm: QueryAlgorithm,
    ) -> io::Result<Self> {
        let inverted_index_path = get_inverted_index_path(index_directory_path.clone());
        let inverted_index_file = File::open(inverted_index_path)?;

        Ok(Self {
            block_cache: CacheType::new_lfu(1000),
            inverted_index_file,
            compression_algorithm,
            query_algorithm,
        })
    }

    pub fn process_query(
        &mut self,
        query_terms: Vec<String>,
        query_metadata: Vec<InMemoryTermMetadata>,
        document_lengths: &Box<[u32]>,
        average_document_length: f32,
    ) -> Vec<(u32, f32)> {
        let mut term_iterators: Vec<TermIterator> = Vec::with_capacity(query_terms.len());
        let mut reader: BufReader<&mut File> = BufReader::new(&mut self.inverted_index_file);

        for i in 0..query_metadata.len() {
            let mut chunks = Vec::new();
            for block_id in query_metadata[i].block_ids {
                if let Some(block) = self.block_cache.get(block_id) {
                    let term_index = block.check_if_term_exists(query_metadata[i].term_id);

                    if term_index == -1 {
                        continue;
                    }
                    chunks.extend(block.decode_chunks_for_term(
                        query_metadata[i].term_id,
                        term_index as usize,
                        self.compression_algorithm.clone(),
                    ));
                } else {
                    let mut new_block = Block::new(*block_id, None);
                    new_block.decode(&mut reader).unwrap();
                    let term_index = new_block.check_if_term_exists(query_metadata[i].term_id);

                    if term_index == -1 {
                        continue;
                    }

                    chunks.extend(new_block.decode_chunks_for_term(
                        query_metadata[i].term_id,
                        term_index as usize,
                        self.compression_algorithm.clone(),
                    ));
                    self.block_cache.put(*block_id, new_block, 1);
                }
            }

            term_iterators.push(TermIterator::new(
                query_terms[i].clone(),
                query_metadata[i].term_id,
                query_metadata[i].term_frequency,
                chunks,
                query_metadata[i].max_score,
                query_metadata[i].chunk_block_max_metadata.unwrap().to_vec(),
            ));
        }
        for term_iterator in &mut term_iterators {
            term_iterator.init();
        }
        match self.query_algorithm {
            QueryAlgorithm::BlockMaxMaxScore => {
                block_max_max_score(term_iterators, document_lengths, average_document_length)
            }
            QueryAlgorithm::BlockMaxWand => {
                block_max_wand(term_iterators, document_lengths, average_document_length)
            }
            QueryAlgorithm::MaxScore => {
                max_score(term_iterators, document_lengths, average_document_length)
            }
            QueryAlgorithm::Wand => wand(term_iterators, document_lengths, average_document_length),
            QueryAlgorithm::Boolean => holistic_binary_merge(term_iterators),
        }
    }
}

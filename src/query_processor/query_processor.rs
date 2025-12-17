use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader},
    u32,
};

use search_engine_cache::CacheType;

use crate::{
    compressor::compressor::CompressionAlgorithm,
    query_processor::{
        retrieval_algorithms::{
            RankingAlgorithm, binary_merge::holistic_binary_merge,
            block_max_max_score::block_max_max_score, block_max_wand::block_max_wand,
            max_score::max_score, wand::wand,
        },
        term_iterator::TermIterator,
    },
    utils::{block::Block, in_memory_term_metadata::InMemoryTermMetadata},
};

pub struct QueryProcessor {
    block_cache: CacheType<u32, Block>,
    inverted_index_file: File,
    compression_algorithm: CompressionAlgorithm,
    ranking_algorithm: RankingAlgorithm,
}

impl QueryProcessor {
    pub fn new(
        compression_algorithm: CompressionAlgorithm,
        ranking_algorithm: RankingAlgorithm,
    ) -> io::Result<Self> {
        let inverted_index_file = File::open("final.idx")?;
        Ok(Self {
            block_cache: CacheType::new_lfu(50),
            inverted_index_file,
            compression_algorithm,
            ranking_algorithm,
        })
    }

    fn get_doc_ids_for_term(&mut self, block_ids: &[u32], term_id: u32) -> HashSet<u32> {
        let mut reader: BufReader<&mut File> = BufReader::new(&mut self.inverted_index_file);

        let mut doc_ids = HashSet::new();
        for i in 0..block_ids.len() {
            let mut block = Block::new(block_ids[i], None);
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
            let mut block = Block::new(block_ids[i], None);
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
        query_metadata: Vec<&InMemoryTermMetadata>,
        document_lengths: &Vec<u32>,
        average_document_length: f32,
    ) -> Vec<u32> {
        let mut term_iterators: Vec<TermIterator> = Vec::new();
        let mut reader: BufReader<&mut File> = BufReader::new(&mut self.inverted_index_file);

        for i in 0..query_metadata.len() {
            let mut chunks = Vec::new();
            for block_id in &query_metadata[i].block_ids {
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
                    new_block.init(&mut reader).unwrap();
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
                query_metadata[i].chunk_block_max_metadata.clone(),
            ));
        }
        for term_iterator in &mut term_iterators {
            term_iterator.init();
        }
        match self.ranking_algorithm {
            RankingAlgorithm::BlockMaxMaxScore => {
                block_max_max_score(term_iterators, document_lengths, average_document_length)
            }
            RankingAlgorithm::BlockMaxWand => {
                block_max_wand(term_iterators, document_lengths, average_document_length)
            }
            RankingAlgorithm::MaxScore => {
                max_score(term_iterators, document_lengths, average_document_length)
            }
            RankingAlgorithm::Wand => {
                wand(term_iterators, document_lengths, average_document_length)
            }
            RankingAlgorithm::Boolean => holistic_binary_merge(term_iterators),
        }
    }
}

use std::{
    fs::{self},
    io::{self, Error, ErrorKind},
    path::Path,
};

use search_engine_cache::CacheType;

use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index::in_memory_index::InMemoryIndex,
    indexer::indexer::{DocumentMetadata, Indexer},
    query_parser::tokenizer::SearchTokenizer,
    query_processor::query_processor::QueryProcessor,
    query_processor::retrieval_algorithms::*,
};

pub struct SearchEngine {
    query_cache: CacheType<String, Vec<u32>>,
    query_processor: QueryProcessor,
    query_parser: SearchTokenizer,
    indexer: Indexer,
    in_memory_index: InMemoryIndex,
    compression_algorithm: CompressionAlgorithm,
    index_directory_path: String,
    result_directory_path: String,
}

impl SearchEngine {
    pub fn new(
        index_directory_path: String,
        compression_algorithm: CompressionAlgorithm,
        result_directory_path: String,
    ) -> Result<Self, Error> {
        let path = Path::new(&index_directory_path);
        if !path.exists() || !path.is_dir() {
            return Err(Error::new(
                ErrorKind::Other,
                "index directory path does not exist, please initialize it ",
            ));
        }
        let path = Path::new(&result_directory_path);
        if !path.exists() || !path.is_dir() {
            fs::remove_dir_all(path)?;
            fs::create_dir_all(path)?;
        }
        let query_parser = SearchTokenizer::new()?;
        let mut indexer = Indexer::new(
            query_parser.clone(),
            compression_algorithm.clone(),
            result_directory_path.clone(),
        )?;
        indexer.set_index_directory_path(index_directory_path.clone());
        let query_processor = QueryProcessor::new(
            compression_algorithm.clone(),
            RankingAlgorithm::BlockMaxMaxScore,
        )?;
        Ok(Self {
            query_cache: CacheType::new_landlord(20),
            query_processor,
            query_parser,
            in_memory_index: InMemoryIndex::new(),
            indexer,
            compression_algorithm,
            index_directory_path,
            result_directory_path,
        })
    }

    pub fn build_index(&mut self) -> io::Result<()> {
        self.in_memory_index = self.indexer.index()?;
        Ok(())
    }

    pub fn set_index_directory_path(&mut self, index_directory_path: String) {
        self.index_directory_path = index_directory_path;
    }

    pub fn set_result_directory_path(&mut self, result_directory_path: String) {
        self.result_directory_path = result_directory_path;
    }
    pub fn get_result_directory_path(&self) -> &String {
        &self.result_directory_path
    }

    pub fn compression_algorithm(&self) -> &CompressionAlgorithm {
        &self.compression_algorithm
    }

    pub fn handle_query(&mut self, query: String) -> Result<Vec<DocumentMetadata>, io::Error> {
        let mut result_metadata = Vec::new();

        if let Some(result_docs) = self.query_cache.get(&query) {
            for doc in result_docs {
                if let Some(metadata) = self.indexer.get_doc_metadata(*doc) {
                    result_metadata.push(metadata);
                }
            }
        } else {
            let token_query_result = self.query_parser.tokenize_query(&query);
            if token_query_result.is_err() {
                return Err(io::Error::new(io::ErrorKind::Unsupported, "error"));
            }

            let tokens = token_query_result.unwrap();
            let mut query_terms = Vec::new();
            let mut query_metadata = Vec::new();
            for token in tokens.unigram {
                query_metadata.push(self.in_memory_index.get_term_metadata(&token.word));
                query_terms.push(token.word);
            }

            let result_docs = self.query_processor.process_query(
                query_terms,
                query_metadata,
                &self.indexer.document_lengths,
                self.indexer.avg_doc_length,
            );

            for doc in &result_docs {
                if let Some(metadata) = self.indexer.get_doc_metadata(*doc) {
                    result_metadata.push(metadata);
                }
            }
            self.query_cache.put(query, result_docs, 0);
        }

        Ok(result_metadata)
    }
}

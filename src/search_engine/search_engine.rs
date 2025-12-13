use std::{
    hash::Hash,
    io::{self, Error, ErrorKind},
    path::Path,
};

use search_engine_cache::{landlord::Landlord, lfu_w::LFUCache, lru::LRUCache, *};

use crate::{
    compressor::compressor::CompressionAlgorithm,
    indexer::indexer::{DocumentMetadata, Indexer},
    query_parser::tokenizer::SearchTokenizer,
    query_processor::query_processor::QueryProcessor,
};

// Option 1: Enum wrapper for selecting cache type at runtime
pub enum CacheType<K, V> {
    LRU(LRUCache<K, V>),
    LFU(LFUCache<K, V>),
    Landlord(Landlord<K, V>),
}

impl<K: Clone + Hash + Eq, V> CacheType<K, V> {
    pub fn new_lru(capacity: usize) -> Self {
        CacheType::LRU(LRUCache::new(capacity))
    }

    pub fn new_lfu(capacity: usize) -> Self {
        CacheType::LFU(LFUCache::new(capacity))
    }

    pub fn new_landlord(capacity: usize) -> Self {
        CacheType::Landlord(Landlord::new(capacity))
    }
}

impl<K: Clone + Hash + Eq, V> Cache<K, V> for CacheType<K, V> {
    fn put(&mut self, key: K, value: V, weight: u32) {
        match self {
            CacheType::LRU(cache) => cache.put(key, value, weight),
            CacheType::LFU(cache) => cache.put(key, value, weight),
            CacheType::Landlord(cache) => cache.put(key, value, weight),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        match self {
            CacheType::LRU(cache) => cache.get(key),
            CacheType::LFU(cache) => cache.get(key),
            CacheType::Landlord(cache) => cache.get(key),
        }
    }

    fn len(&self) -> usize {
        match self {
            CacheType::LRU(cache) => cache.len(),
            CacheType::LFU(cache) => cache.len(),
            CacheType::Landlord(cache) => cache.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            CacheType::LRU(cache) => cache.is_empty(),
            CacheType::LFU(cache) => cache.is_empty(),
            CacheType::Landlord(cache) => cache.is_empty(),
        }
    }

    fn new(capacity: usize) -> Self {
        todo!()
    }
}

pub struct SearchEngine {
    query_cache: CacheType<String, Vec<u32>>,
    query_processor: QueryProcessor,
    query_parser: SearchTokenizer,
    indexer: Indexer,
    compression_algorithm: CompressionAlgorithm,
    index_directory_path: String,
}

impl SearchEngine {
    pub fn new(
        index_directory_path: String,
        compression_algorithm: CompressionAlgorithm,
    ) -> Result<Self, Error> {
        let path = Path::new(&index_directory_path);
        if !path.exists() || !path.is_dir() {
            return Err(Error::new(
                ErrorKind::Other,
                "index directory path does not exist, please initialize it ",
            ));
        }
        let query_parser = SearchTokenizer::new()?;
        let mut indexer = Indexer::new(query_parser.clone(), compression_algorithm.clone())?;
        indexer.set_index_directory(index_directory_path.clone());
        let query_processor = QueryProcessor::new(compression_algorithm.clone())?;
        Ok(Self {
            query_cache: CacheType::new_landlord(20),
            query_processor,
            query_parser,
            indexer,
            compression_algorithm,
            index_directory_path,
        })
    }

    pub fn build_index(&mut self) -> Result<(), io::Error> {
        self.indexer.index()?;
        Ok(())
    }

    pub fn set_index_directory_path(&mut self, index_directory_path: String) {
        self.index_directory_path = index_directory_path;
    }

    pub fn compression_algorithm(&self) -> &CompressionAlgorithm {
        &self.compression_algorithm
    }

    pub fn handle_query(&mut self, query: String) -> Result<Vec<&DocumentMetadata>, io::Error> {
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
                query_metadata.push(self.indexer.get_term_metadata(&token.word));
                query_terms.push(token.word);
            }

            let result_docs = self
                .query_processor
                .process_query(query_terms, query_metadata);

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

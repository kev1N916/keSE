use std::{
    io::{self, Error, ErrorKind},
    path::Path,
};

use crate::{
    compressor::compressor::CompressionAlgorithm,
    indexer::indexer::{DocumentMetadata, Indexer},
    query_parser::tokenizer::SearchTokenizer,
    query_processor::query_processor::QueryProcessor,
};
pub struct SearchEngine {
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

    // pub fn get_postings_from_index(
    //     &self,
    //     posting_offsets: &[PostingOffset],
    // ) -> Result<HashMap<String, (u16, Vec<Posting>)>, io::Error> {

    // }

    pub fn compression_algorithm(&self) -> &CompressionAlgorithm {
        &self.compression_algorithm
    }

    pub fn handle_query(&mut self, query: String) -> Result<Vec<&DocumentMetadata>, io::Error> {
        let token_query_result = self.query_parser.tokenize_query(query);
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

        let mut result_metadata = Vec::new();
        for doc in result_docs {
            if let Some(metadata) = self.indexer.get_doc_metadata(doc) {
                result_metadata.push(metadata);
            }
        }

        Ok(result_metadata)
    }
}

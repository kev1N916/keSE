use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Error, ErrorKind},
    path::Path,
};

use search_engine_cache::CacheType;

use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index_metadata::in_memory_index_metadata::InMemoryIndexMetadata,
    indexer::indexer::{DocumentMetadata, Indexer},
    query_parser::tokenizer::SearchTokenizer,
    query_processor::{query_processor::QueryProcessor, retrieval_algorithms::QueryAlgorithm},
};

pub struct SearchEngine {
    query_cache: CacheType<String, Vec<u32>>,
    query_processor: QueryProcessor,
    query_parser: SearchTokenizer,
    indexer: Indexer,
    in_memory_index_metadata: InMemoryIndexMetadata,
    compression_algorithm: CompressionAlgorithm,
    query_algorithm: QueryAlgorithm,
    index_directory_path: String,
    result_directory_path: String,
}

impl SearchEngine {
    pub fn new(
        index_directory_path: String,
        compression_algorithm: CompressionAlgorithm,
        query_algorithm: QueryAlgorithm,
        result_directory_path: String,
    ) -> Result<Self, Error> {
        let index_path = Path::new(&index_directory_path);
        if !index_path.exists() || !index_path.is_dir() {
            return Err(Error::new(
                ErrorKind::Other,
                "index directory path does not exist, please initialize it ",
            ));
        }
        let result_path = Path::new(&result_directory_path);
        if !result_path.exists() {
            fs::create_dir_all(result_path).unwrap();
        } else if result_path.is_file() {
            fs::create_dir_all(result_path).unwrap();
        }
        let query_parser = SearchTokenizer::new()?;
        let mut indexer = Indexer::new(
            query_parser.clone(),
            compression_algorithm.clone(),
            result_directory_path.clone(),
        )?;

        indexer.set_index_directory_path(index_directory_path.clone());
        let query_processor = QueryProcessor::new(
            result_directory_path.clone(),
            compression_algorithm.clone(),
            query_algorithm.clone(),
        )?;

        Ok(Self {
            query_cache: CacheType::new_landlord(1000),
            query_processor,
            query_parser,
            in_memory_index_metadata: InMemoryIndexMetadata::new(),
            indexer,
            compression_algorithm,
            query_algorithm,
            index_directory_path,
            result_directory_path,
        })
    }

    pub fn build_index(&mut self) -> io::Result<()> {
        self.in_memory_index_metadata = self.indexer.index()?;
        Ok(())
    }

    pub fn load_document_metadata(&mut self) -> io::Result<()> {
        let doc_save_path = Path::new(&self.result_directory_path).join("document_metadata.sidx");
        if !doc_save_path.as_path().exists() {
            return Err(Error::new(
                ErrorKind::InvalidFilename,
                "The document metadata save file does not exist",
            ));
        }
        let file = File::open(&doc_save_path).unwrap();
        let reader = BufReader::new(file);
        self.indexer.load_document_metadata(reader)?;
        Ok(())
    }

    pub fn load_term_metadata(&mut self) -> io::Result<()> {
        let term_save_path = Path::new(&self.result_directory_path).join("term_metadata.sidx");
        if !term_save_path.as_path().exists() {
            return Err(Error::new(
                ErrorKind::InvalidFilename,
                "The term metadata save file does not exist",
            ));
        }
        let file = File::open(&term_save_path).unwrap();
        let reader = BufReader::new(file);
        self.in_memory_index_metadata.load_term_metadata(reader)?;
        Ok(())
    }

    pub fn load_index(&mut self) -> io::Result<()> {
        self.load_document_metadata()?;
        self.load_term_metadata()?;
        Ok(())
    }

    pub fn save_document_metadata(&mut self) -> io::Result<()> {
        let doc_save_path = Path::new(&self.result_directory_path).join("document_metadata.sidx");
        let file = File::create(&doc_save_path.as_path())?;
        let doc_writer = BufWriter::new(file);
        self.indexer.save_document_metadata(doc_writer)?;
        Ok(())
    }

    pub fn save_term_metadata(&mut self) -> io::Result<()> {
        let term_save_path = Path::new(&self.result_directory_path).join("term_metadata.sidx");
        let file = File::create(&term_save_path.as_path())?;
        let term_writer = BufWriter::new(file);
        self.in_memory_index_metadata
            .save_term_metadata(term_writer)?;
        Ok(())
    }

    pub fn save_index(&mut self) -> io::Result<()> {
        self.save_document_metadata()?;
        self.save_term_metadata()?;
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
            let mut query_terms = Vec::with_capacity(tokens.unigram.len());
            let mut query_metadata = Vec::with_capacity(tokens.unigram.len());
            for token in tokens.unigram {
                if let Some(term_metadata) =
                    self.in_memory_index_metadata.get_term_metadata(&token.word)
                {
                    query_metadata.push(term_metadata);
                    query_terms.push(token.word);
                }
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

#[cfg(test)]
mod tests {
    use crate::{
        compressor::compressor::CompressionAlgorithm,
        query_processor::retrieval_algorithms::QueryAlgorithm,
        search_engine::search_engine::SearchEngine,
    };

    #[test]
    fn test_load_index() {
        let mut search_engine = SearchEngine::new(
            "wikipedia".to_string(),
            CompressionAlgorithm::Simple16,
            QueryAlgorithm::Wand,
            "index_run_2".to_string(),
        )
        .unwrap();

        search_engine.load_index().unwrap();
    }
}

use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Error, ErrorKind},
    path::{Path, PathBuf},
};

use search_engine_cache::CacheType;

use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index_metadata::in_memory_index_metadata::InMemoryIndexMetadata,
    indexer::indexer::Indexer,
    parser::parser::Parser,
    query_processor::{query_processor::QueryProcessor, retrieval_algorithms::QueryAlgorithm},
    utils::{
        paths::{get_inverted_index_path, get_save_doc_metadata_path, get_save_term_metadata_path},
        types::{DocumentMetadata, SearchEngineMetadata},
    },
};

pub struct SearchEngine {
    query_cache: CacheType<String, Vec<(u32, f32)>>,
    query_processor: QueryProcessor,
    parser: Parser,
    indexer: Indexer,
    in_memory_index_metadata: InMemoryIndexMetadata,
    compression_algorithm: CompressionAlgorithm,
    query_algorithm: QueryAlgorithm,
    dataset_directory_path: PathBuf,
    index_directory_path: PathBuf,
}

impl SearchEngine {
    pub fn new(
        dataset_directory_path: String,
        compression_algorithm: CompressionAlgorithm,
        query_algorithm: QueryAlgorithm,
        index_directory_path: String,
    ) -> Result<Self, Error> {
        let dataset_path = Path::new(&dataset_directory_path).to_path_buf();
        if !dataset_path.exists() || !dataset_path.is_dir() {
            return Err(Error::new(
                ErrorKind::Other,
                "index directory path does not exist, please initialize it ",
            ));
        }
        let index_path = Path::new(&index_directory_path).to_path_buf();
        if !index_path.exists() {
            fs::create_dir_all(index_path.clone()).unwrap();
        } else if index_path.is_file() {
            fs::create_dir_all(index_path.clone()).unwrap();
        }
        let inverted_index_path = get_inverted_index_path(index_directory_path);
        if !inverted_index_path.exists() {
            if let Err(e) = File::create_new(inverted_index_path) {
                if e.kind() != ErrorKind::AlreadyExists {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Error creating inverted index file: {}", e),
                    ));
                }
            }
        }
        let parser = Parser::new()?;
        let mut indexer = Indexer::new(
            parser.clone(),
            compression_algorithm.clone(),
            index_path.clone(),
        )?;

        indexer.set_dataset_directory_path(dataset_path.clone());
        let query_processor = QueryProcessor::new(
            index_path.clone(),
            compression_algorithm.clone(),
            query_algorithm.clone(),
        )?;

        Ok(Self {
            query_cache: CacheType::new_landlord(10000),
            query_processor,
            parser,
            in_memory_index_metadata: InMemoryIndexMetadata::new(),
            indexer,
            compression_algorithm,
            query_algorithm,
            dataset_directory_path: dataset_path,
            index_directory_path: index_path,
        })
    }

    pub fn merge_spimi_files(&mut self) -> io::Result<()> {
        self.in_memory_index_metadata = self.indexer.merge_spimi_files()?;
        Ok(())
    }
    pub fn build_index(&mut self) -> io::Result<()> {
        self.in_memory_index_metadata = self.indexer.index()?;
        Ok(())
    }

    pub fn load_document_metadata(&mut self) -> io::Result<()> {
        let doc_save_path = get_save_doc_metadata_path(Path::new(&self.index_directory_path));
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
        let term_save_path = get_save_term_metadata_path(Path::new(&self.index_directory_path));
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

    pub fn save_document_metadata(&mut self) -> io::Result<()> {
        let doc_save_path = get_save_doc_metadata_path(Path::new(&self.index_directory_path));
        let file = File::create(&doc_save_path)?;
        let doc_writer = BufWriter::new(file);
        self.indexer.save_document_metadata(doc_writer)?;
        Ok(())
    }

    pub fn save_term_metadata(&mut self) -> io::Result<()> {
        let term_save_path = get_save_term_metadata_path(Path::new(&self.index_directory_path));
        let file = File::create(&term_save_path)?;
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

    pub fn load_index(&mut self) -> io::Result<()> {
        self.load_document_metadata()?;
        self.load_term_metadata()?;
        Ok(())
    }

    pub fn set_dataset_directory_path(&mut self, dataset_directory_path: PathBuf) {
        self.dataset_directory_path = dataset_directory_path;
    }
    pub fn get_dataset_directory_path(&self) -> &str {
        &self.dataset_directory_path.as_os_str().to_str().unwrap()
    }

    pub fn set_index_directory_path(&mut self, index_directory_path: PathBuf) {
        self.index_directory_path = index_directory_path;
    }
    pub fn get_index_directory_path(&self) -> &str {
        &self.index_directory_path.as_os_str().to_str().unwrap()
    }

    pub fn set_compression_algorithm(&mut self, compression_algorithm: CompressionAlgorithm) {
        self.compression_algorithm = compression_algorithm;
    }

    pub fn get_compression_algorithm(&self) -> &CompressionAlgorithm {
        &self.compression_algorithm
    }

    pub fn set_query_algorithm(&mut self, query_algorithm: QueryAlgorithm) {
        self.query_algorithm = query_algorithm;
    }

    pub fn get_query_algorithm(&self) -> &QueryAlgorithm {
        &self.query_algorithm
    }

    pub fn get_index_metadata(&self) -> SearchEngineMetadata {
        let size_of_index = fs::metadata(get_inverted_index_path(self.get_index_directory_path()))
            .unwrap()
            .len() as f64
            / 1_000_000_000.0;
        SearchEngineMetadata {
            no_of_docs: self.indexer.get_no_of_docs(),
            no_of_terms: self.in_memory_index_metadata.no_of_terms,
            no_of_blocks: self.in_memory_index_metadata.no_of_blocks,
            size_of_index: size_of_index,
            dataset_directory_path: self.get_dataset_directory_path().to_string(),
            index_directory_path: self.get_index_directory_path().to_string(),
            compression_algorithm: self.get_compression_algorithm().to_string(),
            query_algorithm: self.get_query_algorithm().to_string(),
        }
    }

    pub fn handle_query(
        &mut self,
        query: String,
    ) -> Result<Vec<(DocumentMetadata, f32)>, io::Error> {
        let mut result_metadata = Vec::new();
        println!("started processing");
        if let Some(result_docs) = self.query_cache.get(&query) {
            for doc in result_docs {
                if let Some(metadata) = self.indexer.get_doc_metadata(doc.0) {
                    result_metadata.push((metadata, doc.1));
                }
            }
        } else {
            let token_query_result = self.parser.tokenize_query(&query);
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
                self.indexer.get_avg_doc_length(),
            );

            for doc in &result_docs {
                if let Some(metadata) = self.indexer.get_doc_metadata(doc.0) {
                    result_metadata.push((metadata, doc.1));
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
    fn test_load_document_metadata() {
        let mut search_engine = SearchEngine::new(
            "wikipedia".to_string(),
            CompressionAlgorithm::Simple16,
            QueryAlgorithm::Wand,
            "index_run_2".to_string(),
        )
        .unwrap();

        search_engine.load_document_metadata().unwrap();
    }

    #[test]
    fn test_save_index() {
        let mut search_engine = SearchEngine::new(
            "wikipedia".to_string(),
            CompressionAlgorithm::Simple16,
            QueryAlgorithm::Wand,
            "index_run_2".to_string(),
        )
        .unwrap();
        search_engine.load_document_metadata().unwrap();
        search_engine.merge_spimi_files().unwrap();
        search_engine.save_index().unwrap();
    }

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

    #[test]
    fn test_query_index() {
        let mut search_engine = SearchEngine::new(
            "wikipedia".to_string(),
            CompressionAlgorithm::Simple16,
            QueryAlgorithm::BlockMaxMaxScore,
            "index_run_2".to_string(),
        )
        .unwrap();

        search_engine.load_index().unwrap();
        let query_string = "misery movie".to_string();
        let results = search_engine.handle_query(query_string).unwrap();
        println!("{:?}", results);
    }
}

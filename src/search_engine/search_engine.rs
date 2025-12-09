use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
    path::Path,
};

use crate::{
    dictionary::Posting,
    indexer::indexer::{DocumentMetadata, Indexer},
    query_parser::tokenizer::SearchTokenizer,
    query_processor::query_processor::QueryProcessor,
};

pub struct QueryResult {
    doc_ids: Vec<u32>,
}

pub struct SearchEngine {
    query_processor: QueryProcessor,
    query_parser: SearchTokenizer,
    indexer: Indexer,
    index_directory_path: String,
}

impl SearchEngine {
    pub fn new(index_directory_path: String) -> Result<Self, Error> {
        let path = Path::new(&index_directory_path);
        if !path.exists() || !path.is_dir() {
            return Err(Error::new(
                ErrorKind::Other,
                "index directory path does not exist, please initialize it ",
            ));
        }
        let query_parser = SearchTokenizer::new()?;
        let mut indexer = Indexer::new(query_parser.clone())?;
        indexer.set_index_directory(index_directory_path.clone());
        let query_processor = QueryProcessor::new()?;
        Ok(Self {
            query_processor,
            index_directory_path,
            query_parser,
            indexer,
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

    fn get_scores_for_docs(
        &self,
        no_of_docs: u32,
        query_terms: &HashMap<String, (u16, Vec<Posting>)>,
    ) -> Vec<f32> {
        let scores: Vec<f32> = vec![0.0; no_of_docs as usize];
        // for (_, (_, posting_list)) in query_terms {
        //     let df: f32 = get_document_frequency(posting_list);
        //     for posting in posting_list {
        //         let tf = get_term_frequency(posting);
        //         let idf = get_inverse_document_frequency(df, self.no_of_docs);
        //         let weight = get_tf_idf_weight(tf, idf);
        //         scores[posting.doc_id as usize] = scores[posting.doc_id as usize] + weight;
        //     }
        // }
        scores
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

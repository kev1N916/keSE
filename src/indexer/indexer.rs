use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index::in_memory_index::InMemoryIndex,
    indexer::spimi::Spmi,
    query_parser::tokenizer::SearchTokenizer,
    utils::{posting::Posting, term::Term},
};
use bzip2::read::BzDecoder;
use pdf_oxide::PdfDocument;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader},
    path::Path,
    sync::mpsc::{self},
};

// Define the structure matching your JSON format
#[derive(Debug, Deserialize, Serialize)]
struct WikiArticle {
    url: String,
    text: Vec<Vec<String>>,
    id: String,
    title: String,
}

#[derive(Clone, Debug)]
pub struct DocumentMetadata {
    pub doc_name: String,
    pub doc_url: String,
    pub doc_length: u32,
}
pub struct Indexer {
    pub avg_doc_length: f32,
    doc_id: u32,
    include_positions: bool,
    document_names: Vec<String>,
    document_urls: Vec<String>,
    pub document_lengths: Vec<u32>,
    index_directory_path: String,
    search_tokenizer: SearchTokenizer,
    compression_algorithm: CompressionAlgorithm,
    result_directory_path: String,
}

fn extract_plaintext(text: &Vec<Vec<String>>) -> String {
    // Join all paragraphs and sentences
    let full_text = text
        .iter()
        .map(|paragraph| paragraph.join(""))
        .collect::<Vec<String>>()
        .join("\n\n"); // Separate paragraphs with double newline

    // Remove all HTML/XML tags using regex
    let tag_regex = Regex::new(r"<[^>]*>").unwrap();
    tag_regex.replace_all(&full_text, "").to_string()
}
impl Indexer {
    pub fn new(
        search_tokenizer: SearchTokenizer,
        compression_algorithm: CompressionAlgorithm,
        result_directory_path: String,
    ) -> Result<Self, std::io::Error> {
        Ok(Self {
            avg_doc_length: 0.0,
            doc_id: 0,
            include_positions: false,
            // document_metadata: HashMap::new(),
            document_lengths: Vec::new(),
            document_names: Vec::new(),
            document_urls: Vec::new(),
            index_directory_path: String::new(),
            search_tokenizer,
            compression_algorithm,
            result_directory_path,
        })
    }

    pub fn get_no_of_docs(&self) -> u32 {
        self.doc_id
    }
    fn read_bz2_file(&mut self, path: &Path, tx: &mpsc::Sender<Term>) -> io::Result<()> {
        let file = File::open(path)?;
        let decoder = BzDecoder::new(file);
        let reader = BufReader::new(decoder);

        // Create a streaming deserializer
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<WikiArticle>();

        // let mut articles = Vec::new();

        for (i, result) in stream.enumerate() {
            match result {
                Ok(article) => {
                    self.doc_id += 1;

                    let plain_text = extract_plaintext(&article.text);
                    let tokens = self.search_tokenizer.tokenize(plain_text);
                    // self.document_metadata.insert(
                    //     self.doc_id,
                    //     DocumentMetadata {
                    //         doc_name: article.title,
                    //         doc_url: article.url,
                    //         doc_length: tokens.len() as u32,
                    //     },
                    // );
                    self.document_names.push(article.title);
                    self.document_urls.push(article.url);
                    self.document_lengths.push(tokens.len() as u32);
                    // articles.push(article);
                    let mut doc_postings: HashMap<String, Vec<u32>> = HashMap::new();
                    for token in &tokens {
                        doc_postings
                            .entry(token.word.clone())
                            .or_insert(Vec::new())
                            .push(token.position);
                    }
                    for (key, value) in doc_postings {
                        let term = Term {
                            posting: Posting {
                                doc_id: self.doc_id,
                                positions: value,
                            },
                            term: key,
                        };
                        tx.send(term).unwrap();
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing object {}: {}", i + 1, e);
                }
            }
        }

        Ok(())
    }

    fn process_directory(&mut self, dir_path: &Path, tx: &mpsc::Sender<Term>) -> io::Result<u32> {
        let mut number_of_articles: u32 = 0;

        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // Recursively process subdirectories
                number_of_articles += self.process_directory(&path, &tx)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("bz2") {
                println!("Processing: {:?}", path);
                self.read_bz2_file(&path, tx)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("pdf") {
                println!("Processing: {:?}", path);
            }
        }

        Ok(number_of_articles)
    }

    pub fn set_index_directory_path(&mut self, index_directory_path: String) {
        self.index_directory_path = index_directory_path;
    }

    pub fn set_result_directory_path(&mut self, result_directory_path: String) {
        self.result_directory_path = result_directory_path;
    }

    pub fn get_index_directory_path(&self) -> String {
        self.index_directory_path.clone()
    }

    pub fn get_result_directory_path(&self) -> String {
        self.result_directory_path.clone()
    }
    pub fn index(&mut self) -> io::Result<InMemoryIndex> {
        let mut spmi = Spmi::new(self.get_result_directory_path());
        let (tx, rx) = mpsc::channel::<Term>();

        let handle = std::thread::spawn(move || {
            spmi.single_pass_in_memory_indexing(rx).unwrap();
        });
        let index_path = self.get_index_directory_path();
        let index_dir = Path::new(&index_path);

        self.process_directory(index_dir, &tx)?;
        drop(tx);
        handle.join().unwrap();
        let mut doc_avg = 0;
        for doc_length in &self.document_lengths {
            doc_avg += doc_length
        }
        self.avg_doc_length = ((doc_avg as f64) / (self.doc_id as f64)) as f32;
        spmi = Spmi::new(self.get_result_directory_path());
        let result = spmi
            .merge_index_files(
                self.avg_doc_length,
                self.include_positions,
                &self.document_lengths,
                self.compression_algorithm.clone(),
                64,
            )
            .unwrap();
        // self.in_memory_index = result;
        Ok(result)
    }

    pub fn get_doc_metadata(&self, doc_id: u32) -> Option<DocumentMetadata> {
        if doc_id <= self.document_lengths.len() as u32 {
            Some(DocumentMetadata {
                doc_name: self.document_names[(doc_id - 1) as usize].clone(),
                doc_url: self.document_urls[(doc_id - 1) as usize].clone(),
                doc_length: self.document_lengths[(doc_id - 1) as usize].clone(),
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_writer() {
        let pdf_dir = Path::new("pdfs");
        let search_tokenizer = SearchTokenizer::new().unwrap();
        for entry in std::fs::read_dir(pdf_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("pdf") {
                let mut doc = PdfDocument::open(path).unwrap();
                let text = doc.extract_text(1).unwrap();
                let tokens = search_tokenizer.tokenize(text);
                for token in tokens {
                    println!("{}", token.word);
                }
            }
        }
    }
}

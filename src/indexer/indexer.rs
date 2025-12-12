use crate::{
    compressors::compressor::CompressionAlgorithm,
    dictionary::{Posting, Term},
    in_memory_dict::map_in_memory_dict::{MapInMemoryDict, MapInMemoryDictPointer},
    indexer::{index_metadata::InMemoryIndexMetatdata, spimi::Spmi},
    my_bk_tree::BkTree,
    query_parser::tokenizer::SearchTokenizer,
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
pub struct IndexMetadata {
    bk_tree: BkTree,
    in_memory_dictionary: MapInMemoryDict,
    term_to_id_map: HashMap<String, u32>,
}

impl IndexMetadata {
    pub fn new() -> Self {
        Self {
            bk_tree: BkTree::new(),
            in_memory_dictionary: MapInMemoryDict::new(),
            term_to_id_map: HashMap::new(),
        }
    }
    pub fn add_term(term: String) {}
}

#[derive(Clone, Debug)]
pub struct DocumentMetadata {
    pub doc_name: String,
    pub doc_url: String,
    pub doc_length: u32,
}
pub struct Indexer {
    l_avg: f32,
    doc_id: u32,
    include_positions: bool,
    document_metadata: HashMap<u32, DocumentMetadata>,
    index_metadata: InMemoryIndexMetatdata,
    index_directory_path: String,
    search_tokenizer: SearchTokenizer,
    compression_algorithm: CompressionAlgorithm,
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
    ) -> Result<Self, std::io::Error> {
        Ok(Self {
            l_avg: 0.0,
            doc_id: 0,
            include_positions: false,
            document_metadata: HashMap::new(),
            index_metadata: InMemoryIndexMetatdata::new(),
            index_directory_path: String::new(),
            search_tokenizer,
            compression_algorithm,
        })
    }

    pub fn get_no_of_docs(&self) -> u32 {
        self.doc_id
    }
    fn read_bz2_file(
        &mut self,
        path: &Path,
        tx: &mpsc::Sender<Term>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
                    self.document_metadata.insert(
                        self.doc_id,
                        DocumentMetadata {
                            doc_name: article.title,
                            doc_url: article.url,
                            doc_length: tokens.len() as u32,
                        },
                    );
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
                    // Optionally break or continue based on your needs
                }
            }
        }

        Ok(())
    }

    fn process_directory(
        &mut self,
        dir_path: &Path,
        tx: &mpsc::Sender<Term>,
    ) -> Result<u32, Box<dyn std::error::Error>> {
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

    pub fn set_index_directory(&mut self, index_directory_path: String) {
        self.index_directory_path = index_directory_path;
    }
    pub fn index(&mut self) -> io::Result<()> {
        let mut spmi = Spmi::new();
        let (tx, rx) = mpsc::channel::<Term>();

        let handle = std::thread::spawn(move || {
            let _ = spmi.single_pass_in_memory_indexing(rx); // Use the moved variable
        });

        let wiki_dir = Path::new("enwiki-20171001-pages-meta-current-withlinks-processed");
        let pdf_dir = Path::new("pdfs");

        let _ = self.process_directory(pdf_dir, &tx);
        drop(tx);
        handle.join().unwrap();
        let mut l_avg = 0;
        for doc in &self.document_metadata {
            l_avg += doc.1.doc_length;
        }
        self.l_avg = ((l_avg as f64) / (self.doc_id as f64)) as f32;
        spmi = Spmi::new();
        let result = spmi
            .merge_index_files(
                self.l_avg,
                self.doc_id,
                self.include_positions,
                &self.document_metadata,
                self.compression_algorithm.clone(),
                64,
            )
            .unwrap();
        self.index_metadata = result;
        Ok(())
    }

    pub fn get_term_metadata(&self, term: &str) -> &MapInMemoryDictPointer {
        self.index_metadata.get_term_metadata(term)
    }

    pub fn get_doc_metadata(&self, doc_id: u32) -> Option<&DocumentMetadata> {
        self.document_metadata.get(&doc_id)
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

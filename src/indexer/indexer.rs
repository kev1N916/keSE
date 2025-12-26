use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index::in_memory_index::InMemoryIndex,
    indexer::spimi::spimi::Spimi,
    query_parser::tokenizer::SearchTokenizer,
    utils::{posting::Posting, term::Term},
};

use once_cell::sync::Lazy;
use regex::Regex;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{self, BufReader, Cursor, Read},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
        mpsc::{self},
    },
    thread,
    time::SystemTime,
};
use zstd::stream::read::Decoder;

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

static TAG_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]*>").unwrap());

fn extract_plaintext(text: &[Vec<String>]) -> String {
    // Pre-calculate capacity to avoid reallocations
    let total_len: usize = text
        .iter()
        .map(|para| para.iter().map(|s| s.len()).sum::<usize>())
        .sum();
    let mut result = String::with_capacity(total_len + text.len() * 2);
    for (i, paragraph) in text.iter().enumerate() {
        if i > 0 {
            result.push_str("\n\n");
        }
        for sentence in paragraph {
            result.push_str(sentence);
        }
    }
    TAG_REGEX.replace_all(&result, "").into_owned()
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

    pub fn encode_metadata(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write avg_doc_length (4 bytes)
        buffer.extend_from_slice(&self.avg_doc_length.to_le_bytes());

        // Write doc_id (4 bytes)
        buffer.extend_from_slice(&self.doc_id.to_le_bytes());

        // Write number of documents (4 bytes)
        buffer.extend_from_slice(&(self.document_lengths.len() as u32).to_le_bytes());

        // Write each document's data consecutively
        for i in 0..self.document_lengths.len() {
            let document_length = &self.document_lengths[i];
            let document_name = &self.document_names[i];
            let document_url = &self.document_urls[i];

            // Write document_length (4 bytes)
            buffer.extend_from_slice(&document_length.to_le_bytes());

            // Write document_name length and bytes
            let name_bytes = document_name.as_bytes();
            buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(name_bytes);

            // Write document_url length and bytes
            let url_bytes = document_url.as_bytes();
            buffer.extend_from_slice(&(url_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(url_bytes);
        }

        buffer
    }

    pub fn decode_metadata(
        bytes: &[u8],
    ) -> Result<
        (
            f32,
            u32,
            CompressionAlgorithm,
            Vec<u32>,
            Vec<String>,
            Vec<String>,
        ),
        String,
    > {
        let mut cursor = std::io::Cursor::new(bytes);
        use std::io::Read;

        // Read avg_doc_length (4 bytes)
        let mut buf = [0u8; 4];
        cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let avg_doc_length = f32::from_le_bytes(buf);

        // Read doc_id (4 bytes)
        cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let doc_id = u32::from_le_bytes(buf);

        // Read compression algorithm (1 byte)
        let mut algo_byte = [0u8; 1];
        cursor
            .read_exact(&mut algo_byte)
            .map_err(|e| e.to_string())?;
        let compression_algorithm = CompressionAlgorithm::from_byte(algo_byte[0])?;

        // Read number of documents (4 bytes)
        cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let num_documents = u32::from_le_bytes(buf) as usize;

        // Pre-allocate vectors
        let mut document_lengths = Vec::with_capacity(num_documents);
        let mut document_names = Vec::with_capacity(num_documents);
        let mut document_urls = Vec::with_capacity(num_documents);

        // Read each document's data consecutively
        for _ in 0..num_documents {
            // Read document_length (4 bytes)
            cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
            let document_length = u32::from_le_bytes(buf);
            document_lengths.push(document_length);

            // Read document_name
            cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
            let name_len = u32::from_le_bytes(buf) as usize;
            let mut name_bytes = vec![0u8; name_len];
            cursor
                .read_exact(&mut name_bytes)
                .map_err(|e| e.to_string())?;
            let document_name = String::from_utf8(name_bytes).map_err(|e| e.to_string())?;
            document_names.push(document_name);

            // Read document_url
            cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
            let url_len = u32::from_le_bytes(buf) as usize;
            let mut url_bytes = vec![0u8; url_len];
            cursor
                .read_exact(&mut url_bytes)
                .map_err(|e| e.to_string())?;
            let document_url = String::from_utf8(url_bytes).map_err(|e| e.to_string())?;
            document_urls.push(document_url);
        }

        Ok((
            avg_doc_length,
            doc_id,
            compression_algorithm,
            document_lengths,
            document_names,
            document_urls,
        ))
    }
    fn read_zstd_file(
        path: &Path,
        tx: &mpsc::SyncSender<Vec<Term>>,
        doc_id: &Arc<AtomicU32>,
        doc_lengths: &Arc<Mutex<Vec<u32>>>,
        doc_urls: &Arc<Mutex<Vec<String>>>,
        doc_names: &Arc<Mutex<Vec<String>>>,
        search_tokenizer: &SearchTokenizer,
    ) -> io::Result<()> {
        let file = File::open(path)?;
        let decoder = Decoder::new(file)?;
        let reader = BufReader::new(decoder);

        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<WikiArticle>();
        let mut terms = Vec::with_capacity(50000);

        let mut local_lengths = Vec::with_capacity(400);
        let mut local_names = Vec::with_capacity(400);
        let mut local_urls = Vec::with_capacity(400);
        let mut local_doc_index = 0u32; // Track local doc index for this file
        for result in stream {
            match result {
                Ok(article) => {
                    let mut doc_postings: FxHashMap<&str, Vec<u32>> =
                        FxHashMap::with_capacity_and_hasher(400, Default::default());

                    // unsafe {
                    let plain_text = extract_plaintext(&article.text);
                    let tokens = search_tokenizer.tokenize(&plain_text);

                    local_lengths.push(tokens.len() as u32);
                    local_names.push(article.title);
                    local_urls.push(article.url);
                    for token in &tokens {
                        doc_postings
                            .entry(&token.word)
                            .or_insert_with(Vec::new)
                            .push(token.position);
                    }

                    // Drain to avoid another clone
                    for (key, value) in doc_postings.drain() {
                        let term = Term {
                            posting: Posting {
                                doc_id: local_doc_index,
                                positions: value,
                            },
                            term: key.to_string(),
                        };
                        terms.push(term);
                    }
                    local_doc_index += 1;
                }
                Err(e) => {
                    eprintln!("Error parsing: {}", e);
                }
            }
        }

        // Now acquire locks ONCE and push everything
        let start_doc_id = {
            let mut lengths = doc_lengths.lock().unwrap();
            let mut names = doc_names.lock().unwrap();
            let mut urls = doc_urls.lock().unwrap();

            // Reserve doc_id range atomically
            let start_id = doc_id.fetch_add(local_lengths.len() as u32, Ordering::SeqCst);

            lengths.append(&mut local_lengths);
            names.append(&mut local_names);
            urls.append(&mut local_urls);

            start_id
        }; // Locks released here

        for term in &mut terms {
            term.posting.doc_id = start_doc_id + term.posting.doc_id + 1;
        }

        tx.send(terms).unwrap();

        Ok(())
    }

    pub fn save(&self) {
        assert_eq!(self.document_lengths.len(), self.document_names.len());
        assert_eq!(self.document_lengths.len(), self.document_urls.len());

        let save_path = Path::new(&self.result_directory_path).join("index_save.sidx");
        let file = File::create(save_path.as_path()).unwrap();
        for i in 0..self.document_lengths.len() {}
    }

    fn process_directory(
        dir_path: &Path,
        tx: &mpsc::SyncSender<Vec<Term>>,
        doc_id: &Arc<AtomicU32>,
        doc_lengths: &Arc<Mutex<Vec<u32>>>,
        doc_urls: &Arc<Mutex<Vec<String>>>,
        doc_names: &Arc<Mutex<Vec<String>>>,
        search_tokenizer: &SearchTokenizer,
    ) -> io::Result<()> {
        let current_time = SystemTime::now();

        for entry in std::fs::read_dir(dir_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("zstd") {
                Indexer::read_zstd_file(
                    &path,
                    tx,
                    doc_id,
                    doc_lengths,
                    doc_urls,
                    doc_names,
                    search_tokenizer,
                )?;
            }
        }
        let now_time = SystemTime::now();
        println!(
            "time taken {:?}",
            now_time.duration_since(current_time).unwrap()
        );
        Ok(())
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
        self.spimi().unwrap();
        self.merge_spimi_files()
    }
    fn spimi(&mut self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel::<Vec<Term>>(10); // Only buffer 5 batches
        let files: Vec<_> = std::fs::read_dir(self.get_index_directory_path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();

        let estimated_docs = 6_000_000;
        let doc_id = Arc::new(AtomicU32::new(0));

        let doc_lengths = Arc::new(Mutex::new(Vec::with_capacity(estimated_docs)));
        let doc_names = Arc::new(Mutex::new(Vec::with_capacity(estimated_docs)));
        let doc_urls = Arc::new(Mutex::new(Vec::with_capacity(estimated_docs)));

        let mut spmi = Spimi::new(self.get_result_directory_path());
        let handle = thread::spawn(move || {
            spmi.single_pass_in_memory_indexing(rx).unwrap();
        });

        let num_threads = 2;
        let chunk_size = (files.len() + num_threads - 1) / num_threads;
        let current_time = SystemTime::now();

        let handles: Vec<_> = files
            .chunks(chunk_size)
            .map(|chunk| {
                let chunk = chunk.to_vec();
                let tx = tx.clone();
                let doc_id = Arc::clone(&doc_id);
                let doc_lengths = Arc::clone(&doc_lengths);
                let doc_names = Arc::clone(&doc_names);
                let doc_urls = Arc::clone(&doc_urls);
                let tokenizer = self.search_tokenizer.clone();
                thread::spawn(move || {
                    let mut files_processed = 0;
                    for file in chunk {
                        Self::process_directory(
                            &file,
                            &tx,
                            &doc_id,
                            &doc_lengths,
                            &doc_urls,
                            &doc_names,
                            &tokenizer,
                        )
                        .unwrap();
                        files_processed += 1;
                        println!("Out of {} files, done with {}", chunk_size, files_processed);
                    }
                })
            })
            .collect();

        for (i, handle) in handles.into_iter().enumerate() {
            handle.join().unwrap_or_else(|e| {
                panic!("Thread {} panicked: {:?}", i, e);
            });
        }

        drop(tx);
        handle.join().unwrap();

        let now_time = SystemTime::now();
        println!(
            "time taken to complete the indexing{:?}",
            now_time.duration_since(current_time).unwrap()
        );

        let final_doc_count = doc_id.load(Ordering::SeqCst) as usize;

        // Extract and truncate to actual size
        let mut lengths = Arc::try_unwrap(doc_lengths).unwrap().into_inner().unwrap();
        let mut names = Arc::try_unwrap(doc_names).unwrap().into_inner().unwrap();
        let mut urls = Arc::try_unwrap(doc_urls).unwrap().into_inner().unwrap();

        lengths.truncate(final_doc_count);
        names.truncate(final_doc_count);
        urls.truncate(final_doc_count);
        // let article_metadata = Arc::try_unwrap(article_metadata)
        //     .unwrap()
        //     .into_inner()
        //     .unwrap();
        // self.document_lengths = article_metadata.doc_length;
        // self.document_names = article_metadata.doc_names;
        // self.document_urls = article_metadata.doc_urls;

        self.document_lengths = lengths;
        self.document_names = names;
        self.document_urls = urls;

        self.doc_id = final_doc_count as u32;

        let mut doc_avg = 0;
        for doc_length in &self.document_lengths {
            doc_avg += doc_length
        }
        self.avg_doc_length = ((doc_avg as f64) / (self.doc_id as f64)) as f32;
        Ok(())
    }

    fn merge_spimi_files(&mut self) -> io::Result<InMemoryIndex> {
        let mut spmi = Spimi::new(self.get_result_directory_path());
        let result = spmi
            .merge_spimi_index_files(
                self.avg_doc_length,
                self.include_positions,
                &self.document_lengths,
                self.compression_algorithm.clone(),
                128,
            )
            .unwrap();

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
    use std::fs;

    use super::*;

    #[test]
    fn test_spimi_index() {
        let query_parser = SearchTokenizer::new().unwrap();
        // let temp_dir = TempDir::new().unwrap();
        let result_path = "index_run_3".to_string();
        let path = Path::new(&result_path);
        if !path.exists() {
            fs::create_dir_all(path).unwrap();
        } else if path.is_file() {
            fs::create_dir_all(path).unwrap();
        }
        let mut indexer =
            Indexer::new(query_parser, CompressionAlgorithm::Simple16, result_path).unwrap();
        let index_directory_path =
            Path::new("enwiki-20171001-pages-meta-current-withlinks-processed");
        indexer.set_index_directory_path(index_directory_path.to_str().unwrap().to_string());
        indexer.spimi().unwrap();
    }

    #[test]
    fn test_spimi_merge() {
        let mut spimi = Spimi::new("index_run_3".to_string());
        spimi
            .merge_spimi_index_files(
                300.0,
                false,
                &Vec::new(),
                CompressionAlgorithm::VarByte,
                128,
            )
            .unwrap();
    }
}

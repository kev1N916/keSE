use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index_metadata::in_memory_index_metadata::InMemoryIndexMetadata,
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
    io::{self, BufReader, BufWriter, Cursor, Read, Write},
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

// The Indexer is responsible for performing the single-pass-in-memory-indexing
// It also contains metadata regarding the documents as well as the terms which is
// needed to answer queries.
pub struct Indexer {
    pub avg_doc_length: f32,
    pub doc_id: u32,
    include_positions: bool,
    pub document_names: Vec<String>,
    pub document_urls: Vec<String>,
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
        let mut local_doc_index = 0u32;
        for result in stream {
            match result {
                Ok(article) => {
                    let mut doc_postings: FxHashMap<&str, Vec<u32>> =
                        FxHashMap::with_capacity_and_hasher(400, Default::default());

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

        let start_doc_id = {
            let mut lengths = doc_lengths.lock().unwrap();
            let mut names = doc_names.lock().unwrap();
            let mut urls = doc_urls.lock().unwrap();

            let start_id = doc_id.fetch_add(local_lengths.len() as u32, Ordering::SeqCst);

            lengths.append(&mut local_lengths);
            names.append(&mut local_names);
            urls.append(&mut local_urls);

            start_id
        };

        for term in &mut terms {
            term.posting.doc_id = start_doc_id + term.posting.doc_id + 1;
        }

        tx.send(terms).unwrap();

        Ok(())
    }

    pub fn save_document_metadata<W: Write>(&self, mut writer: W) -> io::Result<()> {
        assert_eq!(self.document_lengths.len(), self.document_names.len());
        assert_eq!(self.document_lengths.len(), self.document_urls.len());
        assert_eq!(self.document_lengths.len() as u32, self.doc_id);

        writer.write_all(&self.doc_id.to_le_bytes())?;
        writer.write_all(&self.avg_doc_length.to_le_bytes())?;

        for i in 0..self.document_lengths.len() {
            let name_bytes = self.document_names[i].as_bytes();
            writer.write_all(&((name_bytes.len() as u32).to_le_bytes()))?;
            writer.write_all(name_bytes)?;
            let url_bytes = self.document_urls[i].as_bytes();
            writer.write_all(&((url_bytes.len() as u32).to_le_bytes()))?;
            writer.write_all(url_bytes)?;
            writer.write_all(&self.document_lengths[i].to_le_bytes())?;
        }

        writer.flush()?;
        Ok(())
    }

    pub fn load_document_metadata<R: Read>(&mut self, mut reader: R) -> io::Result<()> {
        let mut buffer: [u8; 4] = [0; 4];
        reader.read_exact(&mut buffer)?;
        self.doc_id = u32::from_le_bytes(buffer);

        reader.read_exact(&mut buffer)?;
        self.avg_doc_length = f32::from_le_bytes(buffer);

        self.document_lengths.reserve(self.doc_id as usize);
        self.document_names.reserve(self.doc_id as usize);
        self.document_urls.reserve(self.doc_id as usize);

        for _ in 0..self.doc_id {
            reader.read_exact(&mut buffer)?;
            let name_length = u32::from_le_bytes(buffer) as usize;
            let mut name_buffer: Vec<u8> = vec![0; name_length];
            reader.read_exact(&mut name_buffer)?;
            self.document_names
                .push(String::from_utf8(name_buffer).unwrap());

            reader.read_exact(&mut buffer)?;
            let url_length = u32::from_le_bytes(buffer) as usize;
            let mut url_buffer: Vec<u8> = vec![0; url_length];
            reader.read_exact(&mut url_buffer)?;
            self.document_urls
                .push(String::from_utf8(url_buffer).unwrap());

            reader.read_exact(&mut buffer)?;
            self.document_lengths.push(u32::from_le_bytes(buffer));
        }

        Ok(())
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

    // we create the temporary index files through start_spimi
    // and then merge them
    pub fn index(&mut self) -> io::Result<InMemoryIndexMetadata> {
        self.start_spimi().unwrap();
        self.merge_spimi_files()
    }

    // Starts the spmi function in another thread and then starts processing the directory
    // which we need to index
    fn start_spimi(&mut self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel::<Vec<Term>>(10);
        let files: Vec<_> = std::fs::read_dir(self.get_index_directory_path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();

        let estimated_docs = 6_000_000;

        // We use an instance of doc_id which is passed to the indexing threads
        // This currently makes it faster
        let doc_id = Arc::new(AtomicU32::new(0));
        // The doc metadata in the form of arrays is also passed to the threads
        let doc_lengths = Arc::new(Mutex::new(Vec::with_capacity(estimated_docs)));
        let doc_names = Arc::new(Mutex::new(Vec::with_capacity(estimated_docs)));
        let doc_urls = Arc::new(Mutex::new(Vec::with_capacity(estimated_docs)));

        let mut spmi = Spimi::new(self.get_result_directory_path());
        // the spimi function is started
        let handle = thread::spawn(move || {
            spmi.single_pass_in_memory_indexing(rx).unwrap();
        });

        let num_threads = 2;
        let chunk_size = (files.len() + num_threads - 1) / num_threads;
        let current_time = SystemTime::now();

        // the files are divided based on the number of threads
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

        lengths.shrink_to_fit();
        names.shrink_to_fit();
        urls.shrink_to_fit();
        lengths.truncate(final_doc_count);
        names.truncate(final_doc_count);
        urls.truncate(final_doc_count);

        self.document_lengths = lengths;
        self.document_names = names;
        self.document_urls = urls;

        self.doc_id = final_doc_count as u32;

        // the average length of the documents is calculated as
        // it is needed during the processing of queries
        let mut doc_avg = 0;
        for doc_length in &self.document_lengths {
            doc_avg += doc_length
        }
        self.avg_doc_length = ((doc_avg as f64) / (self.doc_id as f64)) as f32;
        Ok(())
    }

    pub fn merge_spimi_files(&mut self) -> io::Result<InMemoryIndexMetadata> {
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
    fn test_start_spimi() {
        let query_parser = SearchTokenizer::new().unwrap();
        // let temp_dir = TempDir::new().unwrap();
        let result_path = "index_run_2".to_string();
        let path = Path::new(&result_path);
        if !path.exists() {
            fs::create_dir_all(path).unwrap();
        } else if path.is_file() {
            fs::create_dir_all(path).unwrap();
        }
        let mut indexer =
            Indexer::new(query_parser, CompressionAlgorithm::Simple16, result_path).unwrap();
        let index_directory_path = Path::new("wikipedia");
        indexer.set_index_directory_path(index_directory_path.to_str().unwrap().to_string());
        indexer.start_spimi().unwrap();
    }

    #[test]
    fn test_save_document_metadata() {
        let query_parser = SearchTokenizer::new().unwrap();
        // let temp_dir = TempDir::new().unwrap();
        let result_path = "index_run_2".to_string();
        let path = Path::new(&result_path);
        if !path.exists() {
            fs::create_dir_all(path).unwrap();
        } else if path.is_file() {
            fs::create_dir_all(path).unwrap();
        }
        let mut indexer = Indexer::new(
            query_parser,
            CompressionAlgorithm::Simple16,
            result_path.clone(),
        )
        .unwrap();
        let index_directory_path = Path::new("wikipedia");
        indexer.set_index_directory_path(index_directory_path.to_str().unwrap().to_string());
        indexer.start_spimi().unwrap();

        let doc_save_path = path.join("document_metadata.sidx");
        let file = File::create(&doc_save_path.as_path()).unwrap();
        let term_writer = BufWriter::new(file);
        indexer.save_document_metadata(term_writer).unwrap();
    }

    #[test]
    fn test_load_document_metadata() {
        let query_parser = SearchTokenizer::new().unwrap();
        // let temp_dir = TempDir::new().unwrap();
        let result_path = "index_run_2".to_string();
        let path = Path::new(&result_path);
        if !path.exists() {
            fs::create_dir_all(path).unwrap();
        } else if path.is_file() {
            fs::create_dir_all(path).unwrap();
        }
        let mut indexer = Indexer::new(
            query_parser,
            CompressionAlgorithm::Simple16,
            result_path.clone(),
        )
        .unwrap();
        let index_directory_path = Path::new("wikipedia");
        indexer.set_index_directory_path(index_directory_path.to_str().unwrap().to_string());

        let doc_save_path = path.join("document_metadata.sidx");
        let file = File::open(&doc_save_path.as_path()).unwrap();
        let term_writer = BufReader::new(file);

        indexer.load_document_metadata(term_writer).unwrap();

        println!("{} {}", indexer.doc_id, indexer.document_lengths.len());

        for i in 0..indexer.doc_id as usize {
            println!(
                "{} {} {}",
                indexer.document_lengths[i], indexer.document_names[i], indexer.document_urls[i]
            );
        }
    }

    #[test]
    fn test_spimi_merge() {
        let mut spimi = Spimi::new("index_run_2".to_string());
        spimi
            .merge_spimi_index_files(
                300.0,
                false,
                &Vec::new(),
                CompressionAlgorithm::PforDelta,
                128,
            )
            .unwrap();
    }
}

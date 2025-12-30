use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index_metadata::in_memory_index_metadata::InMemoryIndexMetadata,
    indexer::{helper::read_zstd_file, spimi::spimi::Spimi},
    parser::parser::Parser,
    utils::{term::Term, types::DocumentMetadata},
};

use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
        mpsc::{self},
    },
    thread,
    time::SystemTime,
};

// The Indexer is responsible for performing the single-pass-in-memory-indexing
// It also contains metadata regarding the documents as well as the terms which is
// needed to answer queries.
pub struct Indexer {
    avg_doc_length: f32,
    no_of_docs: u32,
    include_positions: bool,
    pub document_names: Box<[String]>,
    pub document_urls: Box<[String]>,
    pub document_lengths: Box<[u32]>,
    parser: Parser,
    compression_algorithm: CompressionAlgorithm,
    index_directory_path: PathBuf,
    dataset_directory_path: PathBuf,
}

impl Indexer {
    pub fn new(
        parser: Parser,
        compression_algorithm: CompressionAlgorithm,
        index_directory_path: PathBuf,
    ) -> Result<Self, std::io::Error> {
        Ok(Self {
            avg_doc_length: 0.0,
            no_of_docs: 0,
            include_positions: false,
            document_lengths: Box::new([]),
            document_names: Box::new([]),
            document_urls: Box::new([]),
            dataset_directory_path: PathBuf::new(),
            parser,
            compression_algorithm,
            index_directory_path,
        })
    }

    pub fn get_no_of_docs(&self) -> u32 {
        self.no_of_docs
    }

    pub fn get_avg_doc_length(&self) -> f32 {
        self.avg_doc_length
    }

    pub fn save_document_metadata<W: Write>(&self, mut writer: W) -> io::Result<()> {
        assert_eq!(self.document_lengths.len(), self.document_names.len());
        assert_eq!(self.document_lengths.len(), self.document_urls.len());
        assert_eq!(self.document_lengths.len() as u32, self.no_of_docs);

        writer.write_all(&self.no_of_docs.to_le_bytes())?;
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
        self.no_of_docs = u32::from_le_bytes(buffer);

        reader.read_exact(&mut buffer)?;
        self.avg_doc_length = f32::from_le_bytes(buffer);

        let mut document_lengths = Vec::with_capacity(self.no_of_docs as usize);
        let mut document_names = Vec::with_capacity(self.no_of_docs as usize);
        let mut document_urls = Vec::with_capacity(self.no_of_docs as usize);

        for _ in 0..self.no_of_docs {
            reader.read_exact(&mut buffer)?;
            let name_length = u32::from_le_bytes(buffer) as usize;
            let mut name_buffer: Vec<u8> = vec![0; name_length];
            reader.read_exact(&mut name_buffer)?;
            document_names.push(String::from_utf8(name_buffer).unwrap());

            reader.read_exact(&mut buffer)?;
            let url_length = u32::from_le_bytes(buffer) as usize;
            let mut url_buffer: Vec<u8> = vec![0; url_length];
            reader.read_exact(&mut url_buffer)?;
            document_urls.push(String::from_utf8(url_buffer).unwrap());

            reader.read_exact(&mut buffer)?;
            document_lengths.push(u32::from_le_bytes(buffer));
        }

        self.document_lengths = document_lengths.into_boxed_slice();
        self.document_names = document_names.into_boxed_slice();
        self.document_urls = document_urls.into_boxed_slice();

        Ok(())
    }

    fn process_directory(
        dir_path: &Path,
        tx: &mpsc::SyncSender<Vec<Term>>,
        doc_id: &Arc<AtomicU32>,
        doc_lengths: &Arc<Mutex<Vec<u32>>>,
        doc_urls: &Arc<Mutex<Vec<String>>>,
        doc_names: &Arc<Mutex<Vec<String>>>,
        search_tokenizer: &Parser,
    ) -> io::Result<()> {
        let current_time = SystemTime::now();

        for entry in std::fs::read_dir(dir_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("zstd") {
                read_zstd_file(
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

    pub fn set_dataset_directory_path(&mut self, dataset_directory_path: PathBuf) {
        self.dataset_directory_path = dataset_directory_path;
    }

    pub fn set_index_directory_path(&mut self, index_directory_path: PathBuf) {
        self.index_directory_path = index_directory_path;
    }

    pub fn get_dataset_directory_path(&self) -> &str {
        &self
            .dataset_directory_path
            .as_os_str()
            .to_str()
            .unwrap_or_default()
    }

    pub fn get_index_directory_path(&self) -> &str {
        &self
            .index_directory_path
            .as_os_str()
            .to_str()
            .unwrap_or_default()
    }

    // we create the temporary index files and then merge them
    pub fn index(&mut self) -> io::Result<InMemoryIndexMetadata> {
        self.start_spimi().unwrap();
        self.merge_spimi_files()
    }

    // Starts the spmi function in another thread and then starts processing the directory
    // which we need to index
    fn start_spimi(&mut self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel::<Vec<Term>>(10);
        let files: Vec<_> = std::fs::read_dir(self.get_dataset_directory_path())
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

        let mut spmi = Spimi::new(self.get_index_directory_path().to_string());
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
                let tokenizer = self.parser.clone();
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

        let mut lengths = Arc::try_unwrap(doc_lengths).unwrap().into_inner().unwrap();
        let mut names = Arc::try_unwrap(doc_names).unwrap().into_inner().unwrap();
        let mut urls = Arc::try_unwrap(doc_urls).unwrap().into_inner().unwrap();

        lengths.truncate(final_doc_count);
        names.truncate(final_doc_count);
        urls.truncate(final_doc_count);

        self.document_lengths = lengths.into_boxed_slice();
        self.document_names = names.into_boxed_slice();
        self.document_urls = urls.into_boxed_slice();

        self.no_of_docs = final_doc_count as u32;

        // the average length of the documents is calculated as
        // it is needed during the processing of queries
        let mut doc_avg = 0;
        for doc_length in &self.document_lengths {
            doc_avg += doc_length
        }
        self.avg_doc_length = ((doc_avg as f64) / (self.no_of_docs as f64)) as f32;
        Ok(())
    }

    pub fn merge_spimi_files(&mut self) -> io::Result<InMemoryIndexMetadata> {
        let mut spmi = Spimi::new(self.get_index_directory_path().to_string());
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
    use std::{
        fs::{self, File},
        io::{BufReader, BufWriter},
    };

    use super::*;

    #[test]
    fn test_start_spimi() {
        let query_parser = Parser::new().unwrap();
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
            path.to_path_buf(),
        )
        .unwrap();
        let dataset_directory_path = Path::new("wikipedia");
        indexer.set_dataset_directory_path(dataset_directory_path.to_path_buf());
        indexer.start_spimi().unwrap();
    }

    #[test]
    fn test_save_document_metadata() {
        let query_parser = Parser::new().unwrap();
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
            path.to_path_buf(),
        )
        .unwrap();
        let dataset_directory_path = Path::new("wikipedia");
        indexer.set_dataset_directory_path(dataset_directory_path.to_path_buf());
        indexer.start_spimi().unwrap();

        let doc_save_path = path.join("document_metadata.sidx");
        let file = File::create(&doc_save_path.as_path()).unwrap();
        let term_writer = BufWriter::new(file);
        indexer.save_document_metadata(term_writer).unwrap();
    }

    #[test]
    fn test_load_document_metadata() {
        let query_parser = Parser::new().unwrap();
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
            path.to_path_buf(),
        )
        .unwrap();
        let dataset_directory_path = Path::new("wikipedia");
        indexer.set_dataset_directory_path(dataset_directory_path.to_path_buf());

        let doc_save_path = path.join("document_metadata.sidx");
        let file = File::open(&doc_save_path.as_path()).unwrap();
        let term_writer = BufReader::new(file);

        indexer.load_document_metadata(term_writer).unwrap();

        println!("{} {}", indexer.no_of_docs, indexer.document_lengths.len());

        for i in 0..indexer.no_of_docs as usize {
            println!(
                "{} {} {}",
                indexer.document_lengths[i], indexer.document_names[i], indexer.document_urls[i]
            );
        }
    }
}

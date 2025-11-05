use std::{
    collections::{BinaryHeap, HashMap},
    fs::{self, File},
    io::{self, Read, Seek},
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::{dictionary::{Dictionary, Posting}, indexer::{helper::vb_decode_posting_list, index_merge_iterator::merge_index_files, spimi::write_block_to_disk}, my_bk_tree::BkTree, positional_intersect::find_documents_optimized, scoring::{ScoredDoc, get_document_frequency, get_inverse_document_frequency, get_term_frequency, get_tf_idf_weight}, tokenizer::SearchTokenizer};

pub struct QueryResult {
    doc_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PostingOffset {
    pub word: String,
    pub posting_offset: i32,
}
impl PostingOffset {
    pub fn new(word: String, posting_offset: i32) -> Self {
        Self {
            word: word,
            posting_offset: posting_offset,
        }
    }
}
pub struct SearchEngine {
    bk_tree: BkTree,
    no_of_docs: u32,
    index_file: Option<Mutex<File>>,
    block_id: u8,
    doc_id: u32,
    query_parser: SearchTokenizer,
    doc_length: Vec<f32>,
    // in_memory_index: Arc<Mutex<InMemoryDict>>,
    indexing_dictionary: Arc<Mutex<Dictionary>>,
    index_path: String,
}

impl SearchEngine {
    pub fn new(block_size: u8, index_path: String) -> Result<Self, io::Error> {
        let query_parser = match SearchTokenizer::new() {
            Ok(parser) => parser,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Tokenization error",
                ));
            }
        };
        Ok(Self {
            bk_tree: BkTree::new(),
            no_of_docs: 0,
            index_file: None,
            block_id: 1,
            doc_id: 0,
            doc_length: Vec::new(),
            query_parser: query_parser,
            // in_memory_index: Arc::new(Mutex::new(InMemoryDict::new(block_size))),
            indexing_dictionary: Arc::new(Mutex::new(Dictionary::new())),
            index_path: index_path,
        })
    }

    fn scan_index_directory(&self, directory: &str) -> Result<Vec<File>, io::Error> {
        let mut file_handles = Vec::new();

        let entries = fs::read_dir(directory)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(extension) = path.extension() {
                    if extension == "txt" {
                        let file = File::open(&path)?;
                        file_handles.push(file);
                    }
                }
            }
        }

        Ok(file_handles)
    }

    pub fn init(&mut self) -> Result<(), io::Error> {
        let files_to_index = self
            .scan_index_directory(&self.index_path)
            .unwrap_or(Vec::new());
        for mut file in files_to_index {
            let mut buffer = String::new();
            file.read_to_string(&mut buffer)?;
            let tokens = self.query_parser.tokenize(buffer);
            if tokens.is_err() {
                return Err(io::Error::new(io::ErrorKind::Unsupported, ""));
            }
            self.doc_id = self.doc_id + 1;
            let mut doc_postings: HashMap<String, Vec<u32>> = HashMap::new();
            for token in &tokens.unwrap() {
                doc_postings
                    .entry(token.word.clone())
                    .or_insert(Vec::new())
                    .push(token.position);
            }
            for (term, positions) in doc_postings {
                let mut dictionary = self.indexing_dictionary.lock().unwrap();
                dictionary.add_term(&term);
                dictionary.append_to_term(
                    &term,
                    Posting {
                        doc_id: self.doc_id,
                        positions: positions,
                    },
                );
            }
            {
                let mut dictionary = self.indexing_dictionary.lock().unwrap();
                if dictionary.get_size() > 4096 {
                    write_block_to_disk(
                        &(self.block_id.to_string() + ".idx"),
                        &dictionary.sort_terms(),
                        &dictionary,
                    )?;
                    dictionary.clear();
                    self.block_id = self.block_id + 1;
                }
            }
        }

        let merge_index_result = merge_index_files(4)?;
        // let Some(merge_index) = merge_index_result else {
        //     return Err(io::Error::new(io::ErrorKind::Unsupported, ""));
        // };

        // self.doc_length = merge_index.doc_lengths;
        // self.bk_tree = merge_index.bk_tree;
        let index_file = File::open("final.idx")?;
        self.index_file = Some(Mutex::new(index_file));
        // self.in_memory_index = Arc::new(Mutex::new(merge_index.in_memory_dict));
        Ok(())
    }

    pub fn get_postings_from_index(
        &self,
        posting_offsets: &[PostingOffset],
    ) -> Result<HashMap<String, (u16, Vec<Posting>)>, io::Error> {
        let mut postings: HashMap<String, (u16, Vec<Posting>)> = HashMap::new();
        let file_mutex = self.index_file.as_ref().unwrap();
        let mut file = file_mutex.lock().unwrap();

        for posting_offset in posting_offsets {
            file.seek(io::SeekFrom::Start(posting_offset.posting_offset as u64))?;
            // Read term length
            let mut term_len_bytes = [0u8; 4];
            file.read_exact(&mut term_len_bytes)?;
            let term_len = u32::from_le_bytes(term_len_bytes) as usize;

            // Read term
            let mut term_bytes = vec![0u8; term_len];
            file.read_exact(&mut term_bytes)?;
            let term = String::from_utf8(term_bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if term != posting_offset.word {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "posting offset incorrect",
                ));
            }

            if postings.contains_key(&term) {
                let post = postings.get_mut(&term);
                match post {
                    Some(post) => {
                        post.0 = post.0 + 1;
                        continue;
                    }
                    None => {}
                }
            }
            // Read encoded posting list length
            let mut posting_list_len_bytes = [0u8; 4];
            file.read_exact(&mut posting_list_len_bytes)?;
            let posting_list_len = u32::from_le_bytes(posting_list_len_bytes) as usize;

            // Read encoded posting list
            let mut encoded_posting_list = vec![0u8; posting_list_len];
            file.read_exact(&mut encoded_posting_list)?;

            // Decode posting list
            let posting_list = vb_decode_posting_list(&encoded_posting_list);
            postings.insert(term, (1, posting_list));
        }

        Ok(postings)
    }

    fn get_scores_for_docs(&self, query_terms: &HashMap<String, (u16, Vec<Posting>)>) -> Vec<f32> {
        let mut scores: Vec<f32> = vec![0.0; self.no_of_docs as usize];
        for (_, (_, posting_list)) in query_terms {
            let df: f32 = get_document_frequency(posting_list);
            for posting in posting_list {
                let tf = get_term_frequency(posting);
                let idf = get_inverse_document_frequency(df, self.no_of_docs);
                let weight = get_tf_idf_weight(tf, idf);
                scores[posting.doc_id as usize] = scores[posting.doc_id as usize] + weight;
            }
        }
        scores
    }
    pub fn handle_query(&self, query: String) -> Result<(), io::Error> {
        let token_query_result = self.query_parser.tokenize_query(query);
        if token_query_result.is_err() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "error"));
        }

        let tokens = token_query_result.unwrap();

        let mut unigram_posting_offsets: Vec<PostingOffset> = Vec::new();
        let mut bigram_posting_offsets: Vec<PostingOffset> = Vec::new();

        let mut unigram_words = Vec::new();
        let mut bigram_words = Vec::new();

        for token in &tokens.unigram {
            // let posting_offset = self.in_memory_index.lock().unwrap().find(&token.word);
            let word = &token.word;
            unigram_words.push(word.clone());
            // unigram_posting_offsets.push(PostingOffset {
            //     word: word.clone(),
            //     posting_offset: posting_offset,
            // });
        }

        for token in &tokens.bigram {
            // let posting_offset = self.in_memory_index.lock().unwrap().find(&token.word);
            let word = &token.word;
            bigram_words.push(word.clone());
            // bigram_posting_offsets.push(PostingOffset {
            //     word: word.clone(),
            //     posting_offset: posting_offset,
            // });
        }

        let unigram_postings_list: HashMap<String, (u16, Vec<Posting>)> =
            self.get_postings_from_index(&unigram_posting_offsets)?;
        let bigram_postings_list: HashMap<String, (u16, Vec<Posting>)> =
            self.get_postings_from_index(&bigram_posting_offsets)?;

        let unigram_postings_offsets_copy: Vec<PostingOffset> = unigram_posting_offsets.clone();
        let unigram_postings_list_copy: HashMap<String, (u16, Vec<Posting>)> =
            unigram_postings_list.clone();

        let unigram_scores = self.get_scores_for_docs(&unigram_postings_list);
        let unigram_scores_copy = unigram_scores.clone();

        let bigram_scores = self.get_scores_for_docs(&bigram_postings_list);

        // Create channels for communication with each thread
        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();
        let (tx3, rx3) = mpsc::channel();

        // Thread 1: Top-K scoring with BinaryHeap
        let handle1 = thread::spawn(move || {
            let docs =
                find_documents_optimized(unigram_words, &unigram_postings_list, true);

            let mut max_heap: BinaryHeap<ScoredDoc> = BinaryHeap::new();
            for doc_id in docs {
                max_heap.push(ScoredDoc {
                    doc_id: doc_id,
                    score: unigram_scores[(doc_id - 1) as usize],
                });
            }

            let k = 2;
            let mut top_k = Vec::new();
            for _ in 0..k.min(max_heap.len()) {
                if let Some(item) = max_heap.pop() {
                    top_k.push((item.doc_id, item.score));
                }
            }

            tx1.send(top_k).unwrap();
        });

        // Thread 2: Simple sorting approach (alternative ranking)
        let handle2 = thread::spawn(move || {
            let docs =
                find_documents_optimized(bigram_words, &bigram_postings_list, false);

            let mut max_heap: BinaryHeap<ScoredDoc> = BinaryHeap::new();
            for doc_id in docs {
                max_heap.push(ScoredDoc {
                    doc_id: doc_id,
                    score: bigram_scores[(doc_id - 1) as usize],
                });
            }

            let k = 2;
            let mut top_k = Vec::new();
            for _ in 0..k.min(max_heap.len()) {
                if let Some(item) = max_heap.pop() {
                    top_k.push((item.doc_id, item.score));
                }
            }

            tx2.send(top_k).unwrap();
        });

        // Thread 3: Statistical analysis of scores
        let handle3 = thread::spawn(move || {
            // let docs = find_documents_optimized(
            //     &unigram_postings_offsets_copy,
            //     &unigram_postings_list_copy,
            //     false,
            // );

            let mut max_heap: BinaryHeap<ScoredDoc> = BinaryHeap::new();
            // for doc_id in docs {
            //     max_heap.push(ScoredDoc {
            //         doc_id: doc_id,
            //         score: unigram_scores_copy[(doc_id - 1) as usize],
            //     });
            // }

            let k = 2;
            let mut top_k = Vec::new();
            for _ in 0..k.min(max_heap.len()) {
                if let Some(item) = max_heap.pop() {
                    top_k.push((item.doc_id, item.score));
                }
            }

            tx3.send(top_k).unwrap();
        });

        // Wait for all threads to complete and collect results
        let mut phrase_docs: Vec<(u32, f32)> = rx1.recv().unwrap();
        let mut bigram_docs: Vec<(u32, f32)> = rx2.recv().unwrap();
        let mut unigram_docs: Vec<(u32, f32)> = rx3.recv().unwrap();

        let mut query_result: Vec<(u32, f32)> = Vec::new();
        query_result.append(&mut phrase_docs);
        query_result.append(&mut bigram_docs);
        query_result.append(&mut unigram_docs);

        // Optional: Join handles to ensure threads completed properly
        handle1.join().unwrap();
        handle2.join().unwrap();
        handle3.join().unwrap();

        Ok(())
    }
}

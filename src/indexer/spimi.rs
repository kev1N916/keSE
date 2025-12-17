use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::Path,
    sync::mpsc,
};

use crate::{
    compressor::compressor::CompressionAlgorithm,
    dictionary::Dictionary,
    in_memory_index::in_memory_index::InMemoryIndex,
    indexer::{
        helper::vb_encode_posting_list, index_merge_iterator::IndexMergeIterator,
        index_merge_writer::MergedIndexBlockWriter, indexer::DocumentMetadata,
    },
    scoring::bm_25::{BM25Params, compute_term_score},
    utils::{
        chunk_block_max_metadata::ChunkBlockMaxMetadata,
        posting::{Posting, merge_postings},
        term::Term,
    },
};

pub struct Spmi {
    dictionary: Dictionary,
    result_directory_path: String,
}

impl Spmi {
    pub fn new(result_directory_path: String) -> Self {
        Self {
            dictionary: Dictionary::new(),
            result_directory_path,
        }
    }
    pub fn single_pass_in_memory_indexing(
        &mut self,
        rx: mpsc::Receiver<Term>,
    ) -> Result<(), std::io::Error> {
        let mut spmi_index = 0;
        let path = Path::new(&self.result_directory_path);

        while let Ok(term) = rx.recv() {
            let does_term_already_exist = self.dictionary.does_term_already_exist(&term.term);
            if self.dictionary.size() >= self.dictionary.max_size() {
                let sorted_terms = self.dictionary.sort_terms();
                self.write_dictionary_to_disk(
                    path.join(spmi_index.to_string()).as_path(),
                    &sorted_terms,
                    &self.dictionary,
                )?;
                spmi_index += 1;
                println!("currently at {}th in memory dictionary", spmi_index);
                self.dictionary.clear();
            }
            if !does_term_already_exist {
                self.dictionary.add_term(&term.term);
            }
            self.dictionary.append_to_term(&term.term, term.posting);
        }
        let sorted_terms = self.dictionary.sort_terms();
        self.write_dictionary_to_disk(
            path.join(spmi_index.to_string() + ".tmpidx").as_path(),
            &sorted_terms,
            &self.dictionary,
        )?;
        Ok(())
    }

    pub fn merge_index_files(
        &mut self,
        l_avg: f32,
        include_positions: bool,
        document_lengths: &Vec<u32>,
        compression_algorithm: CompressionAlgorithm,
        chunk_size: u8,
    ) -> Result<InMemoryIndex, io::Error> {
        let mut in_memory_index: InMemoryIndex = InMemoryIndex::new();
        let mut merge_iterators = Self::scan_and_create_iterators(&self.result_directory_path)?;
        if merge_iterators.is_empty() {
            return Ok(in_memory_index);
        }
        in_memory_index.no_of_docs = document_lengths.len() as u32;
        let mut no_of_terms: u32 = 0;
        let path = Path::new(&self.result_directory_path);
        let final_index_file = File::create(path.join("inverted_index.idx").as_path())?;
        let mut index_merge_writer: MergedIndexBlockWriter = MergedIndexBlockWriter::new(
            final_index_file,
            Some(chunk_size),
            None,
            include_positions,
            compression_algorithm,
        );
        let params = BM25Params::default();
        loop {
            // Find the smallest current term among all iterators that still have terms
            let smallest_term = merge_iterators
                .iter()
                .filter_map(|it| it.current_term.as_ref())
                .min()
                .cloned();

            // Stop if there are no more terms
            let Some(term) = smallest_term else {
                break;
            };

            no_of_terms = no_of_terms + 1;

            let mut posting_lists: Vec<Vec<Posting>> = Vec::new();
            for it in merge_iterators.iter_mut() {
                if let Some(curr_term) = &it.current_term {
                    if curr_term == &term {
                        if let Some(postings) = &it.current_postings {
                            posting_lists.push(postings.clone());
                        }
                        it.next()?;
                    }
                }
            }

            let mut final_merged = Vec::new();
            for postings in posting_lists {
                final_merged = merge_postings(&final_merged, &postings);
            }
            let f_t = final_merged.len() as u32;
            let mut max_term_score: f32 = 0.0;
            let mut chunk_max_term_score: f32 = 0.0;
            let mut chunk_metadata: Vec<ChunkBlockMaxMetadata> = Vec::new();
            let mut chunk_index = 0;
            for posting in &final_merged {
                let f_dt = posting.positions.len() as u32;
                let l_d = document_lengths[(posting.doc_id - 1) as usize];
                let term_score: f32 =
                    compute_term_score(f_dt, l_d, l_avg, in_memory_index.no_of_docs, f_t, &params);
                max_term_score = max_term_score.max(term_score);
                chunk_max_term_score = chunk_max_term_score.max(term_score);
                if (chunk_index + 1) % chunk_size == 0 {
                    chunk_metadata.push(ChunkBlockMaxMetadata {
                        chunk_last_doc_id: posting.doc_id,
                        chunk_max_term_score,
                    });
                    chunk_max_term_score = 0.0;
                }
                chunk_index += 1;
            }
            if chunk_max_term_score != 0.0 {
                chunk_metadata.push(ChunkBlockMaxMetadata {
                    chunk_last_doc_id: final_merged[f_t as usize - 1].doc_id,
                    chunk_max_term_score,
                });
            }
            index_merge_writer.add_term(no_of_terms, final_merged)?;
            in_memory_index.set_term_id(&term, no_of_terms);
            in_memory_index.set_max_term_score(&term, max_term_score);
            in_memory_index.set_chunk_block_max_metadata(&term, chunk_metadata);

            in_memory_index.add_term_to_bk_tree(term);
        }

        index_merge_writer.close()?;
        for term in in_memory_index.get_all_terms() {
            let term_id = in_memory_index.get_term_id(term.clone());
            if term_id != 0 {
                if let Some(term_metadata) = index_merge_writer.get_term_metadata(term_id) {
                    in_memory_index.set_block_ids(&term, term_metadata.block_ids.clone());
                    in_memory_index.set_term_frequency(&term, term_metadata.term_frequency);
                }
            }
        }
        in_memory_index.no_of_terms = no_of_terms;
        Ok(in_memory_index)
    }

    fn scan_and_create_iterators(directory: &str) -> io::Result<Vec<IndexMergeIterator>> {
        let mut iterators = Vec::new();

        // Read directory entries
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();

            // Check for .idx files
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "tmpidx" {
                        let file = File::open(&path)?;
                        let mut merge_iter = IndexMergeIterator::new(file);
                        merge_iter.init()?; // Initialize the iterator
                        iterators.push(merge_iter);
                        println!("Created iterator for: {}", path.display());
                    }
                }
            }
        }

        Ok(iterators)
    }

    fn write_dictionary_to_disk(
        &self,
        filename: &Path,
        sorted_terms: &Vec<String>,
        dict: &Dictionary,
    ) -> Result<(), std::io::Error> {
        let file = File::create(filename)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&(sorted_terms.len() as u32).to_le_bytes())?;
        for term in sorted_terms {
            if let Some(posting_list) = dict.get_postings(term) {
                self.write_term_to_disk(&mut writer, term, &posting_list)?;
            }
        }

        writer.flush()?;
        Ok(())
    }

    fn write_term_to_disk(
        &self,
        writer: &mut BufWriter<File>,
        term: &str,
        posting_list: &Vec<Posting>,
    ) -> Result<(), std::io::Error> {
        writer.write_all(&(term.len() as u32).to_le_bytes())?;
        writer.write_all(term.as_bytes())?;
        let encoded_posting_list = vb_encode_posting_list(posting_list);
        writer.write_all(&(encoded_posting_list.len() as u32).to_le_bytes())?;
        writer.write_all(&encoded_posting_list)?;
        Ok(())
    }
}

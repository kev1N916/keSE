use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufWriter, Write},
    sync::mpsc,
};

use crate::{
    compressors::compressor::CompressionAlgorithm,
    dictionary::{Dictionary, Posting, Term},
    indexer::{
        helper::vb_encode_posting_list, index_merge_iterator::IndexMergeIterator,
        index_merge_writer::MergedIndexBlockWriter, index_metadata::InMemoryIndexMetatdata,
        indexer::DocumentMetadata,
    },
    positional_intersect::merge_postings,
    scoring::bm_25::{BM25Params, compute_term_score},
};

pub struct Spmi {
    dictionary: Dictionary,
}

impl Spmi {
    pub fn new() -> Self {
        Self {
            dictionary: Dictionary::new(),
        }
    }
    pub fn single_pass_in_memory_indexing(
        &mut self,
        rx: mpsc::Receiver<Term>,
    ) -> Result<(), std::io::Error> {
        while let Ok(term) = rx.recv() {
            let does_term_already_exist = self.dictionary.does_term_already_exist(&term.term);
            if self.dictionary.size() >= self.dictionary.max_size() {
                let sorted_terms = self.dictionary.sort_terms();
                self.write_dictionary_to_disk("", &sorted_terms, &self.dictionary)?;
                self.dictionary.clear();
            }
            if !does_term_already_exist {
                self.dictionary.add_term(&term.term);
            }
            self.dictionary.append_to_term(&term.term, term.posting);
        }
        let sorted_terms = self.dictionary.sort_terms();
        self.write_dictionary_to_disk("", &sorted_terms, &self.dictionary)?;
        Ok(())
    }

    pub fn merge_index_files(
        &mut self,
        l_avg: f32,
        n: u32,
        include_positions: bool,
        document_metadata: &HashMap<u32, DocumentMetadata>,
        compression_algorithm: CompressionAlgorithm,
        block_size: u8,
    ) -> Result<InMemoryIndexMetatdata, io::Error> {
        let mut in_memory_index_metadata: InMemoryIndexMetatdata = InMemoryIndexMetatdata::new();
        let final_index_file = File::create("final.idx")?;
        let mut merge_iterators = Self::scan_and_create_iterators("index_directory")?;
        if merge_iterators.is_empty() {
            return Ok(in_memory_index_metadata);
        }
        let mut no_of_terms: u32 = 0;
        let mut index_merge_writer: MergedIndexBlockWriter = MergedIndexBlockWriter::new(
            final_index_file,
            Some(block_size),
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
            for posting in &final_merged {
                let f_dt = posting.positions.len() as u32;
                let l_d = document_metadata.get(&posting.doc_id).unwrap().doc_length;
                let term_score: f32 = compute_term_score(f_dt, l_d, l_avg, n, f_t, &params);
                max_term_score = max_term_score.max(term_score);
            }
            index_merge_writer.add_term(no_of_terms, final_merged)?;
            in_memory_index_metadata.set_term_id(&term, no_of_terms);
            in_memory_index_metadata.set_max_term_score(&term, max_term_score);
            in_memory_index_metadata.add_term_to_bk_tree(term);
        }

        index_merge_writer.close()?;
        for term in in_memory_index_metadata.get_all_terms() {
            let term_id = in_memory_index_metadata.get_term_id(term.clone());
            if term_id != 0 {
                if let Some(term_metadata) = index_merge_writer.get_term_metadata(term_id) {
                    in_memory_index_metadata.set_block_ids(&term, term_metadata.block_ids.clone());
                    in_memory_index_metadata
                        .set_term_frequency(&term, term_metadata.term_frequency);
                }
            }
        }
        Ok(in_memory_index_metadata)
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
                    if ext == "idx" {
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
        filename: &str,
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
        return Ok(());
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

use std::{
    f32,
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
    sync::mpsc,
    time::SystemTime,
};

use crate::{
    compressor::compressor::CompressionAlgorithm,
    in_memory_index_metadata::in_memory_index_metadata::InMemoryIndexMetadata,
    indexer::{
        helper::vb_encode_posting_list,
        spimi::{spimi_iterator::SpimiIterator, spimi_merge_writer::SpimiMergeWriter},
    },
    // parser::parser::is_valid_token,
    scoring::bm_25::{BM25Params, compute_term_score},
    utils::{
        chunk_block_max_metadata::ChunkBlockMaxMetadata,
        dictionary::Dictionary,
        paths::get_inverted_index_path,
        posting::{Posting, merge_all_postings},
        term::Term,
    },
};

// Single Pass In Memory Indexing is performed by accumulating posting lists in memory
// and writing to the disk after this memory exceeds a certain size.
// The size of this dictionary is set according to us, currently it is 200 mb.
// We write multiple dictionaries to the disk which act as temporary inverted indexes and then finally
// perform a merge over all these temprary indexes.
// Since the dictionaries are sorted before writing to the disk performing a merge is easy.
pub struct Spimi {
    dictionary: Dictionary,
    index_directory_path: String,
}

impl Spimi {
    pub fn new(index_directory_path: String) -> Self {
        Self {
            dictionary: Dictionary::new(),
            index_directory_path,
        }
    }

    // We receive vectors which contain posting lists through a channel and write it to our
    // in memory dictionary. Once the dictionary exceeds a maximum size it is written to disk.
    // All the temporary indexes can be identified through the .tmpidx file name.
    pub fn single_pass_in_memory_indexing(
        &mut self,
        rx: mpsc::Receiver<Vec<Term>>,
    ) -> Result<(), std::io::Error> {
        let mut spmi_index = 0;
        let path = Path::new(&self.index_directory_path);

        while let Ok(terms) = rx.recv() {
            for term in terms {
                if self.dictionary.size() >= self.dictionary.max_size() {
                    self.write_dictionary_to_disk(
                        path.join(spmi_index.to_string() + ".tmpidx").as_path(),
                        &self.dictionary,
                    )?;
                    spmi_index += 1;
                    self.dictionary.clear();
                }

                self.dictionary.add_term(&term.term);
                self.dictionary.append_to_term(&term.term, term.posting);
            }
        }

        // Once the channel is closed there may still be unwritten posting lists in the dictionary
        // which have to be flushed to disk.
        self.write_dictionary_to_disk(
            path.join(spmi_index.to_string() + ".tmpidx").as_path(),
            &self.dictionary,
        )?;
        Ok(())
    }

    // Merges the temporary index files produced by the SPIMI run into a final file which is written to inverted_index.idx
    // It produces an InMemoryIndex which contains metadata related to the final inverted index file which is used during
    // query processing.
    pub fn merge_spimi_index_files(
        &mut self,
        l_avg: f32,
        include_positions: bool,
        document_lengths: &Box<[u32]>,
        compression_algorithm: CompressionAlgorithm,
        chunk_size: u8,
    ) -> Result<InMemoryIndexMetadata, io::Error> {
        let current_time = SystemTime::now();
        let mut in_memory_index_metadata: InMemoryIndexMetadata = InMemoryIndexMetadata::new();

        // Iterators are created over our temporary index files
        let mut merge_iterators =
            SpimiIterator::scan_and_create_iterators(&self.index_directory_path)?;
        if merge_iterators.is_empty() {
            return Ok(in_memory_index_metadata);
        }
        let mut no_of_terms: u32 = 0;
        let path = Path::new(&self.index_directory_path);
        let final_index_file = File::create(get_inverted_index_path(path).as_path())?;

        // The index writer is used to efficiently create our inverted index
        let mut spimi_merge_writer: SpimiMergeWriter = SpimiMergeWriter::new(
            final_index_file,
            Some(chunk_size),
            None,
            include_positions,
            compression_algorithm,
        );
        // The BM25 scoring params are created.
        let bm25_params = BM25Params::default();
        let no_of_docs = document_lengths.len() as u32;
        loop {
            // We iterate over our iterators to find the smallest term
            // Since the size of the vector is quite small I have chosen to just loop over it.
            let smallest_term = merge_iterators
                .iter()
                .filter_map(|it| it.current_term.as_ref())
                .min()
                .cloned();

            // Stop if there are no more terms
            let Some(term) = smallest_term else {
                break;
            };

            // The posting lists from the different iterators are accumulated and then merged
            // to create the final posting list for the current term.
            let mut posting_lists: Vec<Vec<Posting>> = Vec::with_capacity(50);
            for it in merge_iterators.iter_mut() {
                if let Some(curr_term) = &it.current_term {
                    if curr_term == &term {
                        if let Some(postings) = it.current_postings.take() {
                            posting_lists.push(postings);
                        }
                        it.next()?;
                    }
                }
            }

            let final_merged = merge_all_postings(posting_lists);

            no_of_terms += 1;

            // The term_frequency is calculated for ranked retrieval
            let term_frequency = final_merged.len() as u32;

            // The max_term_score is used for WAND ranked retrieval and needs to be calculated here
            // and stored as metadata
            let mut max_term_score: f32 = f32::MIN;
            // The chunk_max_term_score is used for BLOCK_MAX ranked retrieval algorithms and needs to be calculated here
            // and stored as metadata
            let mut chunk_max_term_score: f32 = f32::MIN;
            let mut chunk_metadata: Vec<ChunkBlockMaxMetadata> = Vec::new();
            let mut chunk_index: usize = 0;

            for posting in &final_merged {
                let f_dt = posting.positions.len() as u32;
                let l_d = document_lengths[(posting.doc_id - 1) as usize];
                // We compute the contribution of this document to the term_score
                let term_score: f32 =
                    compute_term_score(f_dt, l_d, l_avg, no_of_docs, term_frequency, &bm25_params);
                // The document may contribute to the max_term_score
                max_term_score = max_term_score.max(term_score);

                // The chunk_max_term_score is calculated but it is only added to the BlockMaxMetadata
                // after the chunk is completed
                chunk_max_term_score = chunk_max_term_score.max(term_score);
                if (chunk_index + 1) % chunk_size as usize == 0 {
                    chunk_metadata.push(ChunkBlockMaxMetadata::new(
                        posting.doc_id,
                        chunk_max_term_score,
                    ));
                    chunk_max_term_score = f32::MIN;
                }
                chunk_index += 1;
            }
            if chunk_max_term_score != f32::MIN {
                chunk_metadata.push(ChunkBlockMaxMetadata::new(
                    final_merged[term_frequency as usize - 1].doc_id,
                    chunk_max_term_score,
                ));
            }

            chunk_metadata.shrink_to_fit();

            let block_ids = spimi_merge_writer
                .add_term(no_of_terms, final_merged)
                .unwrap();

            // We add the term to term_id mapping, the max_term_score the and the metadata for
            // block max ranking to the in memory index.
            in_memory_index_metadata.set_term_id(term, no_of_terms);
            in_memory_index_metadata.set_term_frequency(term_frequency);
            in_memory_index_metadata.set_max_term_score(max_term_score);
            in_memory_index_metadata.set_chunk_block_max_metadata(chunk_metadata);
            in_memory_index_metadata.set_block_ids(block_ids);
            // We add the term to the bk_tree as well which helps speed up retrieval
            // in_memory_index.add_term_to_bk_tree(term);
        }

        // We close the index_merge_writer so that the remaining terms can be written to the disk.
        spimi_merge_writer.close()?;
        in_memory_index_metadata.close();

        // We keep track of total no of blocks and total no of terms
        in_memory_index_metadata.no_of_blocks = spimi_merge_writer.current_block_no;
        in_memory_index_metadata.no_of_terms = no_of_terms;
        let now_time = SystemTime::now();
        println!(
            "time taken to complete merge index file{:?} with total number of terms {}",
            now_time.duration_since(current_time),
            no_of_terms
        );
        Ok(in_memory_index_metadata)
    }

    fn write_dictionary_to_disk(
        &self,
        filename: &Path,
        dict: &Dictionary,
    ) -> Result<(), std::io::Error> {
        if dict.no_of_terms > 0 {
            let file = File::create(filename)?;
            let mut writer = BufWriter::new(file);
            writer.write_all(&(dict.no_of_terms).to_le_bytes())?;
            for (key, value) in &dict.dictionary {
                self.write_term_to_disk(&mut writer, key, value)?;
            }
            writer.flush()?;
        }
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

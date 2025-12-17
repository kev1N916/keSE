use std::{
    f32,
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
        index_merge_writer::MergedIndexBlockWriter,
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
            if self.dictionary.size() >= self.dictionary.max_size() {
                let sorted_terms = self.dictionary.sort_terms();
                self.write_dictionary_to_disk(
                    path.join(spmi_index.to_string() + ".tmpidx").as_path(),
                    &sorted_terms,
                    &self.dictionary,
                )?;
                spmi_index += 1;
                self.dictionary.clear();
            }
            let does_term_already_exist = self.dictionary.does_term_already_exist(&term.term);
            if !does_term_already_exist {
                self.dictionary.add_term(&term.term);
            }
            self.dictionary.append_to_term(&term.term, term.posting);
        }
        let sorted_terms = self.dictionary.sort_terms();
        if sorted_terms.len() > 0 {
            self.write_dictionary_to_disk(
                path.join(spmi_index.to_string() + ".tmpidx").as_path(),
                &sorted_terms,
                &self.dictionary,
            )?;
        }
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
            let mut max_term_score: f32 = f32::MIN;
            let mut chunk_max_term_score: f32 = f32::MIN;
            let mut chunk_metadata: Vec<ChunkBlockMaxMetadata> = Vec::new();
            let mut chunk_index: usize = 0;
            for posting in &final_merged {
                let f_dt = posting.positions.len() as u32;
                let l_d = document_lengths[(posting.doc_id - 1) as usize];
                let term_score: f32 =
                    compute_term_score(f_dt, l_d, l_avg, in_memory_index.no_of_docs, f_t, &params);
                max_term_score = max_term_score.max(term_score);
                chunk_max_term_score = chunk_max_term_score.max(term_score);

                if (chunk_index + 1) % chunk_size as usize == 0 {
                    chunk_metadata.push(ChunkBlockMaxMetadata {
                        chunk_last_doc_id: posting.doc_id,
                        chunk_max_term_score,
                    });
                    chunk_max_term_score = f32::MIN;
                }
                chunk_index += 1;
            }
            if chunk_max_term_score != f32::MIN {
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
            let term_id = in_memory_index.get_term_id(&term);
            if term_id != 0 {
                if let Some(term_metadata) = index_merge_writer.get_term_metadata(term_id) {
                    in_memory_index.set_block_ids(&term, term_metadata.block_ids.clone());
                    in_memory_index.set_term_frequency(&term, term_metadata.term_frequency);
                }
            }
        }
        in_memory_index.no_of_blocks = index_merge_writer.current_block_no;
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

#[cfg(test)]
mod tests {
    use crate::indexer::helper::vb_decode_posting_list;
    use crate::utils::block::Block;

    use super::*;
    use std::fs;
    use std::io::{BufReader, Read};
    use std::sync::mpsc;
    use std::thread;
    use tempfile::TempDir;

    // Helper function to create test terms
    fn create_term(term: &str, doc_id: u32, positions: Vec<u32>) -> Term {
        Term {
            term: term.to_string(),
            posting: Posting::new(doc_id, positions),
        }
    }

    // Helper function to read and verify dictionary file
    fn read_dictionary_file(path: &Path) -> Result<Vec<(String, Vec<Posting>)>, std::io::Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut num_terms_bytes = [0u8; 4];
        reader.read_exact(&mut num_terms_bytes)?;
        let num_terms = u32::from_le_bytes(num_terms_bytes);

        let mut terms = Vec::new();

        for _ in 0..num_terms {
            // Read term length
            let mut term_len_bytes = [0u8; 4];
            reader.read_exact(&mut term_len_bytes)?;
            let term_len = u32::from_le_bytes(term_len_bytes);

            // Read term
            let mut term_bytes = vec![0u8; term_len as usize];
            reader.read_exact(&mut term_bytes)?;
            let term = String::from_utf8(term_bytes).unwrap();

            // Read posting list length
            let mut postings_len_bytes = [0u8; 4];
            reader.read_exact(&mut postings_len_bytes)?;
            let postings_len = u32::from_le_bytes(postings_len_bytes);

            // Read encoded posting list
            let mut encoded_postings = vec![0u8; postings_len as usize];
            reader.read_exact(&mut encoded_postings)?;

            // Decode posting list
            let postings = vb_decode_posting_list(&encoded_postings);

            terms.push((term, postings));
        }

        Ok(terms)
    }

    #[test]
    fn test_spmi_small_number_of_terms() {
        // Create temporary directory
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        // Spawn thread to process terms
        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send a small number of terms (less than dictionary max size)
        tx.send(create_term("apple", 1, vec![10, 20])).unwrap();
        tx.send(create_term("banana", 1, vec![15])).unwrap();
        tx.send(create_term("apple", 2, vec![5, 25, 35])).unwrap();
        tx.send(create_term("cherry", 3, vec![8])).unwrap();
        tx.send(create_term("banana", 2, vec![12, 22])).unwrap();

        drop(tx); // Close channel to signal completion

        // Wait for processing to complete
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // Verify output file exists
        let index_file = Path::new(&result_path).join("0.tmpidx");
        assert!(index_file.exists(), "Index file should be created");

        // Read and verify the dictionary file
        let terms = read_dictionary_file(&index_file).unwrap();

        // Should have 3 unique terms
        assert_eq!(terms.len(), 3, "Should have 3 unique terms");

        // Verify terms are sorted alphabetically
        assert_eq!(terms[0].0, "apple");
        assert_eq!(terms[1].0, "banana");
        assert_eq!(terms[2].0, "cherry");

        // Verify posting lists
        assert_eq!(terms[0].1.len(), 2, "Apple should have 2 postings");
        assert_eq!(terms[1].1.len(), 2, "Banana should have 2 postings");
        assert_eq!(terms[2].1.len(), 1, "Cherry should have 1 posting");
    }

    #[test]
    fn test_spmi_large_number_of_terms_multiple_flushes() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send a large number of terms to trigger multiple dictionary flushes
        // Assuming dictionary max size is around 10000-100000 terms
        let num_unique_terms = 10000;
        let terms_per_doc = 5;

        for i in 0..num_unique_terms {
            let term = format!("term_{:06}", i);
            for doc in 1..=terms_per_doc {
                tx.send(create_term(&term, doc, vec![i as u32])).unwrap();
            }
        }

        drop(tx);

        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // Verify multiple index files were created
        let entries: Vec<_> = fs::read_dir(&result_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "tmpidx")
                    .unwrap_or(false)
            })
            .collect();

        assert!(entries.len() >= 1, "Should create at least one index file");
        println!("Created {} index files", entries.len());

        // Collect all terms from all files
        let mut all_terms_found = std::collections::HashSet::new();
        let mut total_postings_count = std::collections::HashMap::new();

        for entry in entries {
            let terms = read_dictionary_file(&entry.path()).unwrap();
            assert!(terms.len() > 0, "Each file should contain terms");

            // Verify terms are sorted within each file
            for i in 1..terms.len() {
                assert!(
                    terms[i - 1].0 <= terms[i].0,
                    "Terms should be sorted alphabetically"
                );
            }

            // Collect terms and their posting counts
            for (term, postings) in terms {
                all_terms_found.insert(term.clone());
                *total_postings_count.entry(term).or_insert(0) += postings.len();
            }
        }

        // Verify all unique terms are present
        assert_eq!(
            all_terms_found.len(),
            num_unique_terms as usize,
            "All {} unique terms should be present across all files",
            num_unique_terms
        );

        // Verify each term has correct number of postings
        for i in 0..num_unique_terms {
            let term = format!("term_{:06}", i);
            assert!(
                all_terms_found.contains(&term),
                "Term '{}' should be present",
                term
            );

            let posting_count = total_postings_count.get(&term).unwrap_or(&0);
            assert_eq!(
                *posting_count, terms_per_doc as usize,
                "Term '{}' should have {} postings",
                term, terms_per_doc
            );
        }

        println!(
            "Successfully verified all {} terms with {} postings each",
            num_unique_terms, terms_per_doc
        );
    }

    #[test]
    fn test_spmi_duplicate_terms_merged() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send same term multiple times with different documents
        for doc_id in 1..=10 {
            tx.send(create_term("test", doc_id, vec![1, 2, 3])).unwrap();
        }

        drop(tx);

        let result = handle.join().unwrap();
        assert!(result.is_ok());

        let index_file = Path::new(&result_path).join("0.tmpidx");
        let terms = read_dictionary_file(&index_file).unwrap();

        assert_eq!(terms.len(), 1, "Should have only 1 unique term");
        assert_eq!(terms[0].0, "test");
        assert_eq!(terms[0].1.len(), 10, "Should have 10 postings for 'test'");
    }

    #[test]
    fn test_spmi_preserves_positions() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send term with specific positions
        let positions = vec![5, 10, 15, 20, 100, 200];
        tx.send(create_term("word", 1, positions.clone())).unwrap();

        drop(tx);

        let result = handle.join().unwrap();
        assert!(result.is_ok());

        let index_file = Path::new(&result_path).join("0.tmpidx");
        let terms = read_dictionary_file(&index_file).unwrap();

        assert_eq!(
            terms[0].1[0].positions, positions,
            "Positions should be preserved"
        );
    }

    #[test]
    fn test_spmi_different_doc_ids() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send same term for different documents
        tx.send(create_term("common", 1, vec![1])).unwrap();
        tx.send(create_term("common", 100, vec![2])).unwrap();
        tx.send(create_term("common", 1000, vec![3])).unwrap();
        tx.send(create_term("common", 10000, vec![4])).unwrap();

        drop(tx);

        let result = handle.join().unwrap();
        assert!(result.is_ok());

        let index_file = Path::new(&result_path).join("0.tmpidx");
        let terms = read_dictionary_file(&index_file).unwrap();

        assert_eq!(terms[0].1.len(), 4, "Should have 4 postings");

        // Verify doc IDs
        let doc_ids: Vec<u32> = terms[0].1.iter().map(|p| p.doc_id).collect();
        assert_eq!(doc_ids, vec![1, 100, 1000, 10000]);
    }

    #[test]
    fn test_spmi_boundary_flush() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send terms that will trigger boundary conditions
        // This tests the flush logic when dictionary reaches max size
        for i in 0..40000 {
            tx.send(create_term(&format!("term{}", i), 1, vec![i]))
                .unwrap();
        }

        drop(tx);

        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // Count files created
        let file_count = fs::read_dir(&result_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "tmpidx")
                    .unwrap_or(false)
            })
            .count();

        println!("Created {} index files for boundary test", file_count);
        assert!(file_count >= 1);
    }

    #[test]
    fn test_scan_and_create_iterators_single_file() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send some terms
        tx.send(create_term("alpha", 1, vec![1, 2])).unwrap();
        tx.send(create_term("beta", 1, vec![3, 4])).unwrap();
        tx.send(create_term("gamma", 2, vec![5])).unwrap();

        drop(tx);
        handle.join().unwrap().unwrap();

        // Now test scan_and_create_iterators
        let iterators = Spmi::scan_and_create_iterators(&result_path).unwrap();

        assert_eq!(iterators.len(), 1, "Should create 1 iterator for 1 file");

        // Verify iterator is initialized with a current term
        assert!(
            iterators[0].current_term.is_some(),
            "Iterator should be initialized with a term"
        );
    }

    #[test]
    fn test_scan_and_create_iterators_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        println!("{:?}", temp_dir);
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send enough terms to trigger multiple flushes
        for i in 0..50000 {
            tx.send(create_term(&format!("word{:05}", i), 1, vec![i]))
                .unwrap();
        }

        drop(tx);
        handle.join().unwrap().unwrap();

        // Test scan_and_create_iterators
        let iterators = Spmi::scan_and_create_iterators(&result_path).unwrap();

        assert!(iterators.len() > 1, "Should create at least 1 iterator");

        println!("Created {} iterators for multiple files", iterators.len());

        // Verify all iterators are properly initialized
        for (idx, iterator) in iterators.iter().enumerate() {
            assert!(
                iterator.current_term.is_some(),
                "Iterator {} should be initialized with a term",
                idx
            );
            assert!(
                iterator.current_postings.is_some(),
                "Iterator {} should have postings",
                idx
            );
        }
    }

    // ==================== MERGE_INDEX_FILES TESTS ====================

    #[test]
    fn test_merge_index_files_single_file() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send small set of terms
        tx.send(create_term("apple", 1, vec![10, 20])).unwrap();
        tx.send(create_term("apple", 2, vec![15])).unwrap();
        tx.send(create_term("banana", 1, vec![5])).unwrap();
        tx.send(create_term("cherry", 3, vec![30, 40, 50])).unwrap();

        drop(tx);
        handle.join().unwrap().unwrap();

        // Create document lengths (3 documents)
        let document_lengths = vec![100, 150, 200];
        let l_avg = 150.0;

        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                false, // include_positions
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128, // chunk_size
            )
            .unwrap();

        // Verify basic structure
        assert_eq!(in_memory_index.no_of_docs, 3);
        assert_eq!(in_memory_index.no_of_terms, 3);

        // Verify all terms are present
        assert!(in_memory_index.get_term_id("apple") > 0);
        assert!(in_memory_index.get_term_id("banana") > 0);
        assert!(in_memory_index.get_term_id("cherry") > 0);

        // Verify inverted_index.idx file was created
        let index_file = Path::new(&result_path).join("inverted_index.idx");
        assert!(index_file.exists(), "Merged index file should be created");
    }

    #[test]
    fn test_merge_index_files_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send enough terms to create multiple tmpidx files
        let num_terms = 50000;
        for i in 0..num_terms {
            let term = format!("term{:05}", i);
            tx.send(create_term(&term, (i % 100) + 1, vec![i])).unwrap();
        }

        drop(tx);
        handle.join().unwrap().unwrap();

        // Verify multiple tmpidx files were created
        let tmpidx_count = fs::read_dir(&result_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "tmpidx")
                    .unwrap_or(false)
            })
            .count();

        println!("Created {} tmpidx files", tmpidx_count);
        assert!(tmpidx_count > 1, "Should create multiple tmpidx files");

        // Create document lengths
        let document_lengths = vec![100; 100];
        let l_avg = 100.0;

        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                true,
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128,
            )
            .unwrap();

        // Verify all terms were merged
        assert_eq!(in_memory_index.no_of_terms, num_terms);
        assert_eq!(in_memory_index.no_of_docs, 100);

        // Verify final index file exists
        let index_file = Path::new(&result_path).join("inverted_index.idx");
        assert!(index_file.exists());
    }

    #[test]
    fn test_merge_index_files_duplicate_terms_across_files() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send same terms multiple times across different flushes
        // First batch - will be in first file
        for doc_id in 1..=5000 {
            tx.send(create_term("common", doc_id, vec![1, 2])).unwrap();
        }

        // Force different terms to trigger flush
        for i in 0..10000 {
            tx.send(create_term(&format!("filler{}", i), 1, vec![i]))
                .unwrap();
        }

        // Second batch - will be in second file
        for doc_id in 5001..=10000 {
            tx.send(create_term("common", doc_id, vec![3, 4])).unwrap();
        }

        drop(tx);
        handle.join().unwrap().unwrap();

        let document_lengths = vec![100; 10000];
        let l_avg = 100.0;

        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                true,
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128,
            )
            .unwrap();

        // Verify "common" term was properly merged
        let common_term_id = in_memory_index.get_term_id("common");
        assert!(common_term_id > 0, "Common term should exist");

        // // Verify term frequency
        let term_freq = in_memory_index.get_term_frequency("common".to_string());
        assert_eq!(term_freq, 10000, "Common term should have 10000 postings");
    }

    #[test]
    fn test_merge_index_files_term_ordering() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send terms in random order
        let terms = vec!["zebra", "apple", "mango", "banana", "cherry", "date"];
        for (idx, term) in terms.iter().enumerate() {
            tx.send(create_term(term, (idx + 1) as u32, vec![idx as u32]))
                .unwrap();
        }

        drop(tx);
        handle.join().unwrap().unwrap();

        let document_lengths = vec![100; 6];
        let l_avg = 100.0;

        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                false,
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128,
            )
            .unwrap();

        // Verify all terms exist
        for term in &terms {
            let term_id = in_memory_index.get_term_id(&term);
            assert!(term_id > 0, "Term '{}' should exist", term);
        }

        // Term IDs should be assigned in alphabetical order
        let apple_id = in_memory_index.get_term_id("apple");
        let banana_id = in_memory_index.get_term_id("banana");
        let zebra_id = in_memory_index.get_term_id("zebra");

        assert!(apple_id < banana_id, "Apple should come before banana");
        assert!(banana_id < zebra_id, "Banana should come before zebra");
    }

    #[test]
    fn test_merge_index_files_max_term_scores() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send terms with different frequencies
        // "rare" appears once
        tx.send(create_term("rare", 1, vec![1])).unwrap();

        // "common" appears in multiple docs
        for doc_id in 1..=10 {
            tx.send(create_term("common", doc_id, vec![1, 2, 3]))
                .unwrap();
        }
        drop(tx);
        handle.join().unwrap().unwrap();

        let document_lengths = vec![100; 10];
        let l_avg = 100.0;

        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                true,
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128,
            )
            .unwrap();

        // Verify max term scores are set
        let rare_score = in_memory_index.get_max_term_score("rare".to_string());
        let common_score = in_memory_index.get_max_term_score("common".to_string());

        assert!(rare_score > 0.0, "Rare term should have positive score");
        assert!(
            common_score < 0.0,
            "Common term should have negative score as it appears in many documents"
        );
    }

    #[test]
    fn test_merge_index_files_chunk_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Send a term that appears in many documents
        for doc_id in 1..=500 {
            tx.send(create_term("widespread", doc_id, vec![1])).unwrap();
        }

        drop(tx);
        handle.join().unwrap().unwrap();

        let document_lengths = vec![100; 500];
        let l_avg = 100.0;
        let chunk_size = 128;

        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index: InMemoryIndex = spmi
            .merge_index_files(
                l_avg,
                true,
                &document_lengths,
                CompressionAlgorithm::VarByte,
                chunk_size,
            )
            .unwrap();

        // Verify chunk metadata exists
        let chunk_metadata = in_memory_index.get_chunk_block_max_metadata("widespread");
        assert!(chunk_metadata.is_some(), "Chunk metadata should exist");

        let metadata = chunk_metadata.unwrap();

        // With 500 postings and chunk_size 128, we should have 4 chunks
        // (128, 128, 128, 116)
        let expected_chunks = (500 + chunk_size as usize - 1) / chunk_size as usize;
        assert_eq!(
            metadata.len(),
            expected_chunks,
            "Should have {} chunks",
            expected_chunks
        );

        // Verify chunk properties
        for (idx, chunk) in metadata.iter().enumerate() {
            assert!(
                chunk.chunk_last_doc_id > 0,
                "Chunk {} should have valid last doc ID",
                idx
            );
        }
    }

    #[test]
    fn test_merge_index_files_block_ids() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        tx.send(create_term("test", 1, vec![1, 2, 3])).unwrap();
        tx.send(create_term("test", 2, vec![4, 5])).unwrap();

        drop(tx);
        handle.join().unwrap().unwrap();

        let document_lengths = vec![100; 2];
        let l_avg = 100.0;

        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                true,
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128,
            )
            .unwrap();

        // Verify block IDs are set
        let block_ids = in_memory_index.get_block_ids("test");
        assert!(block_ids.is_some(), "Block IDs should be set");
        assert!(
            block_ids.unwrap().len() > 0,
            "Should have at least one block ID"
        );
    }

    #[test]
    fn test_merge_index_files_without_positions() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        tx.send(create_term("word", 1, vec![10, 20, 30])).unwrap();
        tx.send(create_term("word", 2, vec![15, 25])).unwrap();

        drop(tx);
        handle.join().unwrap().unwrap();

        let document_lengths = vec![100; 2];
        let l_avg = 100.0;

        // Merge without positions
        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                false, // include_positions = false
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128,
            )
            .unwrap();

        assert_eq!(in_memory_index.no_of_terms, 1);
        assert!(Path::new(&result_path).join("inverted_index.idx").exists());
    }

    #[test]
    fn test_merge_index_files_large_scale_with_variable_positions() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        println!("Test directory: {:?}", temp_dir.path());

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Configuration
        let num_unique_terms = 50000;
        let num_documents = 1000;
        let max_docs_per_term = 50; // Each term appears in at most 50 documents

        println!(
            "Generating {} unique terms across {} documents...",
            num_unique_terms, num_documents
        );

        // Use a deterministic random pattern
        for term_idx in 0..num_unique_terms {
            let term = format!("term_{:08}", term_idx);

            // Variable number of documents per term (1 to max_docs_per_term)
            // Using modulo for deterministic pattern
            let num_docs_for_term = (term_idx % max_docs_per_term) + 1;

            // Collect all doc_ids first
            let mut doc_positions: Vec<(u32, Vec<u32>)> = Vec::new();

            for doc_offset in 0..num_docs_for_term {
                // Calculate doc_id using a spread pattern
                let doc_id = ((term_idx * 7 + doc_offset * 13) % num_documents) + 1;

                // Variable length positions based on term and doc
                // Short positions (1-3): common case
                // Medium positions (4-10): moderate case
                // Long positions (11-50): rare case
                let position_count = match term_idx % 10 {
                    0..=5 => (term_idx % 3) + 1, // 60% have 1-3 positions
                    6..=8 => (term_idx % 7) + 4, // 30% have 4-10 positions
                    _ => (term_idx % 40) + 11,   // 10% have 11-50 positions
                };

                // Generate position list
                let mut positions: Vec<u32> = (0..position_count)
                    .map(|i| ((i * 5 + term_idx as u32) % 500) + 1)
                    .collect();

                // Sort positions to ensure ascending order
                positions.sort_unstable();

                println!("postions {:?}", positions);

                doc_positions.push((doc_id as u32, positions));
            }

            // Sort by doc_id to ensure ascending order
            doc_positions.sort_by_key(|(doc_id, _)| *doc_id);
            // Send sorted documents
            for (doc_id, positions) in doc_positions {
                tx.send(create_term(&term, doc_id, positions)).unwrap();
            }

            // Progress indicator
            if term_idx % 10000 == 0 && term_idx > 0 {
                println!("Generated {} terms...", term_idx);
            }
        }
        println!("Finished generating terms, waiting for indexing to complete...");
        drop(tx);
        handle.join().unwrap().unwrap();

        // Verify multiple tmpidx files were created
        let tmpidx_files: Vec<_> = fs::read_dir(&result_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "tmpidx")
                    .unwrap_or(false)
            })
            .collect();

        println!("Created {} tmpidx files", tmpidx_files.len());
        assert!(
            tmpidx_files.len() >= 1,
            "Should create at least one tmpidx file"
        );

        // Create realistic document lengths with variation
        let document_lengths: Vec<u32> = (0..num_documents)
            .map(|i| {
                // Document lengths vary from 50 to 500 words
                let base = 100;
                let variation = (i * 17) % 400; // Deterministic variation
                (base + variation) as u32
            })
            .collect();

        let l_avg: f32 =
            document_lengths.iter().sum::<u32>() as f32 / document_lengths.len() as f32;
        println!("Average document length: {:.2}", l_avg);

        // Perform the merge
        println!("Starting merge of index files...");
        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                false, // include_positions
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128, // chunk_size
            )
            .unwrap();

        println!("Merge completed!");

        // Comprehensive verification
        println!("\n=== Verification Results ===");

        // 1. Verify term count
        assert_eq!(
            in_memory_index.no_of_terms, num_unique_terms,
            "Should have {} unique terms",
            num_unique_terms
        );
        println!("✓ Term count: {}", in_memory_index.no_of_terms);

        // 2. Verify document count
        assert_eq!(
            in_memory_index.no_of_docs, num_documents as u32,
            "Should have {} documents",
            num_documents
        );
        println!("✓ Document count: {}", in_memory_index.no_of_docs);

        // 3. Verify final index file exists
        let final_index = Path::new(&result_path).join("inverted_index.idx");
        assert!(
            final_index.exists(),
            "Final inverted_index.idx should exist"
        );
        let file_size = fs::metadata(&final_index).unwrap().len();
        println!(
            "✓ Final index file size: {:.2} MB",
            file_size as f64 / (1024.0 * 1024.0)
        );

        // 4. Sample verification of specific terms
        let sample_terms = vec![
            "term_00000000",
            "term_00010000",
            "term_00025000",
            "term_00049999",
        ];

        for term in &sample_terms {
            let term_id = in_memory_index.get_term_id(&term);
            assert!(term_id > 0, "Term '{}' should exist with valid ID", term);

            let max_score = in_memory_index.get_max_term_score(term.to_string());
            assert!(max_score != 0.0, "Term '{}' should have a score", term);

            let term_freq = in_memory_index.get_term_frequency(term.to_string());
            assert!(
                term_freq > 0,
                "Term '{}' should have positive frequency",
                term
            );

            let block_ids = in_memory_index.get_block_ids(term);
            assert!(block_ids.is_some(), "Term '{}' should have block IDs", term);

            let chunk_metadata = in_memory_index.get_chunk_block_max_metadata(term);
            assert!(
                chunk_metadata.is_some(),
                "Term '{}' should have chunk metadata",
                term
            );
        }
        println!("✓ Sample term verification passed");

        // 5. Verify score distribution makes sense
        let rare_term_score = in_memory_index.get_max_term_score("term_00000001".to_string());
        let common_term_score = in_memory_index.get_max_term_score("term_00000049".to_string());

        println!(
            "✓ Score examples - term_00000001: {:.4}, term_00000049: {:.4}",
            rare_term_score, common_term_score
        );

        // 6. Verify term frequency distribution
        let mut freq_counts = std::collections::HashMap::new();
        for i in 0..100 {
            let term = format!("term_{:08}", i);
            let freq = in_memory_index.get_term_frequency(term);
            *freq_counts.entry(freq).or_insert(0) += 1;
        }
        println!(
            "✓ Frequency distribution (first 100 terms): {:?}",
            freq_counts
        );

        // 7. Verify chunk metadata exists and is reasonable
        let term_with_many_postings = "term_00000049"; // Should have max_docs_per_term postings
        let chunk_meta = in_memory_index.get_chunk_block_max_metadata(term_with_many_postings);
        if let Some(chunks) = chunk_meta {
            println!(
                "✓ Term '{}' has {} chunks",
                term_with_many_postings,
                chunks.len()
            );
            assert!(chunks.len() > 0, "Should have at least one chunk");

            // Verify chunk properties
            for (idx, chunk) in chunks.iter().enumerate() {
                assert!(
                    chunk.chunk_max_term_score > 0.0,
                    "Chunk {} should have positive max score",
                    idx
                );
                assert!(
                    chunk.chunk_last_doc_id > 0,
                    "Chunk {} should have valid last doc ID",
                    idx
                );
            }
        }
        println!("\n=== All Verifications Passed! ===");
    }

    #[test]
    fn test_merge_index_files_and_reading_from_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let result_path = temp_dir.path().to_str().unwrap().to_string();

        println!("Test directory: {:?}", temp_dir.path());

        let mut spmi = Spmi::new(result_path.clone());
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || spmi.single_pass_in_memory_indexing(rx));

        // Configuration
        let num_unique_terms = 10000;
        let num_documents = 1000;
        let max_docs_per_term = 50; // Each term appears in at most 50 documents

        println!(
            "Generating {} unique terms across {} documents...",
            num_unique_terms, num_documents
        );

        // 4. Sample verification of specific terms
        let sample_terms = vec![
            "term_00000936".to_string(),
            "term_00001865".to_string(),
            "term_00002791".to_string(),
            "term_00003723".to_string(),
            "term_00004645".to_string(),
            "term_00005579".to_string(),
            "term_00006499".to_string(),
            "term_00008363".to_string(),
        ];

        let mut sample_docs = vec![];

        // Use a deterministic random pattern
        for term_idx in 0..num_unique_terms {
            let term = format!("term_{:08}", term_idx);

            // Variable number of documents per term (1 to max_docs_per_term)
            // Using modulo for deterministic pattern
            let num_docs_for_term = (term_idx % max_docs_per_term) + 1;

            // Collect all doc_ids first
            let mut doc_positions: Vec<(u32, Vec<u32>)> = Vec::new();

            for doc_offset in 0..num_docs_for_term {
                // Calculate doc_id using a spread pattern
                let doc_id = ((term_idx * 7 + doc_offset * 13) % num_documents) + 1;

                // Variable length positions based on term and doc
                // Short positions (1-3): common case
                // Medium positions (4-10): moderate case
                // Long positions (11-50): rare case
                let position_count = match term_idx % 10 {
                    0..=5 => (term_idx % 3) + 1, // 60% have 1-3 positions
                    6..=8 => (term_idx % 7) + 4, // 30% have 4-10 positions
                    _ => (term_idx % 40) + 11,   // 10% have 11-50 positions
                };

                // Generate position list
                let mut positions: Vec<u32> = (0..position_count)
                    .map(|i| ((i * 5 + term_idx as u32) % 500) + 1)
                    .collect();

                // Sort positions to ensure ascending order
                positions.sort_unstable();

                doc_positions.push((doc_id as u32, positions));
            }

            // Sort by doc_id to ensure ascending order
            doc_positions.sort_by_key(|(doc_id, _)| *doc_id);
            let mut doc_vec = Vec::new();
            // Send sorted documents
            for (doc_id, positions) in doc_positions {
                if sample_terms.contains(&term) {
                    doc_vec.push(doc_id);
                }
                tx.send(create_term(&term, doc_id, positions)).unwrap();
            }

            if sample_terms.contains(&term) {
                sample_docs.push(doc_vec);
            }

            // Progress indicator
            if term_idx % 10000 == 0 && term_idx > 0 {
                println!("Generated {} terms...", term_idx);
            }
        }
        println!("Finished generating terms, waiting for indexing to complete...");
        drop(tx);
        handle.join().unwrap().unwrap();

        // Verify multiple tmpidx files were created
        let tmpidx_files: Vec<_> = fs::read_dir(&result_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "tmpidx")
                    .unwrap_or(false)
            })
            .collect();

        println!("Created {} tmpidx files", tmpidx_files.len());
        assert!(
            tmpidx_files.len() >= 1,
            "Should create at least one tmpidx file"
        );

        // Create realistic document lengths with variation
        let document_lengths: Vec<u32> = (0..num_documents)
            .map(|i| {
                // Document lengths vary from 50 to 500 words
                let base = 100;
                let variation = (i * 17) % 400; // Deterministic variation
                (base + variation) as u32
            })
            .collect();

        let l_avg: f32 =
            document_lengths.iter().sum::<u32>() as f32 / document_lengths.len() as f32;
        println!("Average document length: {:.2}", l_avg);

        // Perform the merge
        println!("Starting merge of index files...");
        let mut spmi = Spmi::new(result_path.clone());
        let in_memory_index = spmi
            .merge_index_files(
                l_avg,
                false, // include_positions
                &document_lengths,
                CompressionAlgorithm::VarByte,
                128, // chunk_size
            )
            .unwrap();

        println!("Merge completed!");

        // Comprehensive verification
        println!("\n=== Verification Results ===");

        // 1. Verify term count
        assert_eq!(
            in_memory_index.no_of_terms, num_unique_terms,
            "Should have {} unique terms",
            num_unique_terms
        );
        println!("✓ Term count: {}", in_memory_index.no_of_terms);

        // 2. Verify document count
        assert_eq!(
            in_memory_index.no_of_docs, num_documents as u32,
            "Should have {} documents",
            num_documents
        );
        println!("✓ Document count: {}", in_memory_index.no_of_docs);

        // 3. Verify final index file exists
        let final_index = Path::new(&result_path).join("inverted_index.idx");
        assert!(
            final_index.exists(),
            "Final inverted_index.idx should exist"
        );
        let file_size = fs::metadata(&final_index).unwrap().len();
        println!(
            "✓ Final index file size: {:.2} MB",
            file_size as f64 / (1024.0 * 1024.0)
        );

        let mut inverted_index_file = File::open(final_index).unwrap();
        let mut reader = BufReader::new(&mut inverted_index_file);

        for i in 0..sample_docs.len() {
            let sample_term = &sample_terms[i];
            let expected_sample_docs = &sample_docs[i];
            let block_ids = in_memory_index.get_block_ids(&sample_term).unwrap();
            let term_id = in_memory_index.get_term_id(&sample_term);

            let mut gotten_sample_docs = Vec::new();
            for block_id in block_ids {
                let mut block = Block::new(*block_id, None);
                block.init(&mut reader).unwrap();
                let term_index = block.check_if_term_exists(term_id);
                assert!(term_index >= 0);

                let chunks = block.decode_chunks_for_term(
                    term_id,
                    term_index as usize,
                    CompressionAlgorithm::VarByte,
                );
                for chunk in chunks {
                    let vec = chunk.get_doc_ids();
                    for doc in vec {
                        gotten_sample_docs.push(doc);
                    }
                }
            }

            assert_eq!(expected_sample_docs.len(), gotten_sample_docs.len());
        }
    }
}

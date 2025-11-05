use std::{
    fs::{self, File},
    io::{self, Read, Seek},
};

use crate::{
    dictionary::Posting,
    indexer::{
        helper::vb_decode_posting_list, index_merge_writer::MergedIndexBlockWriter,
        index_metadata::InMemoryIndexMetatdata,
    },
    positional_intersect::merge_postings,
};

pub struct IndexMergeIterator {
    no_of_terms: u32,
    file: File,
    current_term_no: u32,
    current_term: Option<String>,
    current_postings: Option<Vec<Posting>>,
    current_offset: u32,
}

impl IndexMergeIterator {
    pub fn new(file: File) -> IndexMergeIterator {
        IndexMergeIterator {
            file: file,
            no_of_terms: 0,
            current_term_no: 0,
            current_term: None,
            current_postings: None,
            current_offset: 0,
        }
    }

    pub fn init(&mut self) -> io::Result<()> {
        self.file.seek(std::io::SeekFrom::Start(0))?;
        let mut buf = [0u8; 4];

        self.file.read_exact(&mut buf)?;

        self.no_of_terms = u32::from_le_bytes(buf);

        self.current_offset = 4;

        self.next()?;

        Ok(())
    }

    pub fn next(&mut self) -> io::Result<bool> {
        if self.current_term_no >= self.no_of_terms {
            self.current_term = None;
            self.current_postings = None;
            return Ok(false);
        }
        let mut buf = [0u8; 4];

        self.file.read_exact(&mut buf)?;
        let string_length = u32::from_le_bytes(buf) as usize;
        self.current_offset += 4;

        let mut string_buf = vec![0u8; string_length];
        self.file.read_exact(&mut string_buf)?;
        self.current_term = Some(
            String::from_utf8(string_buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
        );
        self.current_offset += string_length as u32;

        self.file.read_exact(&mut buf)?;
        let postings_length = u32::from_le_bytes(buf) as usize;
        self.current_offset += 4;

        let mut postings_buf = vec![0u8; postings_length];
        self.file.read_exact(&mut postings_buf)?;
        let posting_list = vb_decode_posting_list(&postings_buf);
        self.current_postings = Some(posting_list);
        self.current_offset += postings_length as u32;

        self.current_term_no += 1;

        Ok(true)
    }
}

pub fn merge_index_files(block_size: u8) -> Result<InMemoryIndexMetatdata, io::Error> {
    let mut in_memory_index_metadata = InMemoryIndexMetatdata::new();
    let final_index_file = File::create("final.idx")?;
    let mut merge_iterators = scan_and_create_iterators("index_directory")?;
    if merge_iterators.is_empty() {
        return Ok(in_memory_index_metadata);
    }
    let mut no_of_terms: u32 = 0;
    let mut index_merge_writer: MergedIndexBlockWriter =
        MergedIndexBlockWriter::new(final_index_file, Some(block_size));
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
        index_merge_writer.add_term(no_of_terms, final_merged)?;
        in_memory_index_metadata.set_term_id(&term, no_of_terms);
        in_memory_index_metadata.add_term_to_bk_tree(term);

        // let df = get_document_frequency(&final_merged);
        // for posting in &final_merged {
        //     let tf = get_term_frequency(posting);
        //     let v = doc_lengths.get_mut(&posting.doc_id);
        //     if v.is_some() {
        //         let vec = v.unwrap();
        //         vec.push(tf * df);
        //     }
        // }
        // posting_offset += 8 + encoded_posting_list.len() as u32;
    }

    for term in in_memory_index_metadata.get_all_terms() {
        let term_id = in_memory_index_metadata.get_term_id(term.clone());
        if term_id != 0 {
            if let Some(term_metadata) = index_merge_writer.get_term_metadata(term_id) {
                in_memory_index_metadata.set_block_ids(&term, term_metadata.block_ids.clone());
                in_memory_index_metadata.set_term_frequency(&term, term_metadata.term_frequency);
            }
        }
    }

    // for doc_id in 1..doc_lengths.len() + 1 {
    //     let mut doc_length: f32 = 0.0;
    //     if let Some(tf_idfs) = doc_lengths.get(&(doc_id as u32)) {
    //         for tf_idf in tf_idfs {
    //             doc_length = doc_length + (tf_idf * tf_idf);
    //         }
    //     }
    //     doc_lengths_final.push(doc_length.sqrt());
    // }

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

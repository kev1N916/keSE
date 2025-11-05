// use std::{
//     collections::HashMap,
//     fs::{self, File},
//     io::{self, BufWriter, Read, Seek, Write},
// };

// use crate::{
//     my_bk_tree::BkTree,
//     dictionary::Posting,
//     in_memory_dict::InMemoryDict,
//     positional_intersect::merge_postings,
//     scoring::{get_document_frequency, get_term_frequency},
//     spimi::{vb_decode_posting_list, vb_encode_posting_list},
// };

// struct MergeIterator {
//     no_of_terms: u32,
//     file: File,
//     current_term_no: u32,
//     current_term: Option<String>,
//     current_postings: Option<Vec<Posting>>,
//     current_offset: u32,
// }

// impl MergeIterator {
//     pub fn new(file: File) -> MergeIterator {
//         MergeIterator {
//             file: file,
//             no_of_terms: 0,
//             current_term_no: 0,
//             current_term: None,
//             current_postings: None,
//             current_offset: 0,
//         }
//     }

//     pub fn init(&mut self) -> io::Result<()> {
//         self.file.seek(std::io::SeekFrom::Start(0))?;
//         let mut buf = [0u8; 4];

//         self.file.read_exact(&mut buf)?;

//         self.no_of_terms = u32::from_le_bytes(buf);

//         self.current_offset = 4;

//         self.next()?;

//         Ok(())
//     }

//     pub fn next(&mut self) -> io::Result<bool> {
//         if self.current_term_no >= self.no_of_terms {
//             self.current_term = None;
//             self.current_postings = None;
//             return Ok(false);
//         }
//         let mut buf = [0u8; 4];

//         self.file.read_exact(&mut buf)?;
//         let string_length = u32::from_le_bytes(buf) as usize;
//         self.current_offset += 4;

//         let mut string_buf = vec![0u8; string_length];
//         self.file.read_exact(&mut string_buf)?;
//         self.current_term = Some(
//             String::from_utf8(string_buf)
//                 .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
//         );
//         self.current_offset += string_length as u32;

//         self.file.read_exact(&mut buf)?;
//         let postings_length = u32::from_le_bytes(buf) as usize;
//         self.current_offset += 4;

//         let mut postings_buf = vec![0u8; postings_length];
//         self.file.read_exact(&mut postings_buf)?;
//         let posting_list = vb_decode_posting_list(&postings_buf);
//         self.current_postings = Some(posting_list);
//         self.current_offset += postings_length as u32;

//         self.current_term_no += 1;

//         Ok(true)
//     }
// }
// fn scan_idx_files(directory: &str) -> io::Result<Vec<File>> {
//     let mut file_handles = Vec::new();

//     // Read the directory entries
//     let entries = fs::read_dir(directory)?;

//     for entry in entries {
//         let entry = entry?;
//         let path = entry.path();

//         // Check if it's a file and has .idx extension
//         if path.is_file() {
//             if let Some(extension) = path.extension() {
//                 if extension == "idx" {
//                     // Open the file and add handle to vector
//                     let file = File::open(&path)?;
//                     file_handles.push(file);
//                     println!("Added file: {}", path.display());
//                 }
//             }
//         }
//     }

//     Ok(file_handles)
// }

// fn scan_and_create_iterators(directory: &str) -> io::Result<Vec<MergeIterator>> {
//     let mut iterators = Vec::new();

//     // Read directory entries
//     for entry in fs::read_dir(directory)? {
//         let entry = entry?;
//         let path = entry.path();

//         // Check for .idx files
//         if path.is_file() {
//             if let Some(ext) = path.extension() {
//                 if ext == "idx" {
//                     let file = File::open(&path)?;
//                     let mut merge_iter = MergeIterator::new(file);
//                     merge_iter.init()?; // Initialize the iterator
//                     iterators.push(merge_iter);
//                     println!("Created iterator for: {}", path.display());
//                 }
//             }
//         }
//     }

//     Ok(iterators)
// }

// pub struct MergeIndexFileResult {
//     pub in_memory_dict: InMemoryDict,
//     pub doc_lengths: Vec<f32>,
//     pub bk_tree: BkTree,
// }
// pub fn merge_index_files(block_size: u8) -> Result<Option<MergeIndexFileResult>, io::Error> {
//     let mut in_memory_dict = InMemoryDict::new(block_size);
//     let mut bk_tree = BkTree::new();
//     let mut doc_lengths_final: Vec<f32> = Vec::new();
//     let final_index = File::create("final.idx")?;
//     let mut writer: BufWriter<File> = BufWriter::new(final_index);
//     writer.seek(io::SeekFrom::Start(4))?;
//     let mut iterators = scan_and_create_iterators("index_directory")?;
//     if iterators.is_empty() {
//         return Ok(None);
//     }
//     let mut no_of_terms: u32 = 0;
//     let mut posting_offset = 4;
//     let mut doc_lengths: HashMap<u32, Vec<f32>> = HashMap::new();
//     loop {
//         // Find the smallest current term among all iterators that still have terms
//         let smallest_term = iterators
//             .iter()
//             .filter_map(|it| it.current_term.as_ref())
//             .min()
//             .cloned();

//         // Stop if there are no more terms
//         let Some(term) = smallest_term else {
//             break;
//         };

//         bk_tree.add(&term);
//         no_of_terms = no_of_terms + 1;

//         let mut posting_lists: Vec<Vec<Posting>> = Vec::new();

//         for it in iterators.iter_mut() {
//             if let Some(curr_term) = &it.current_term {
//                 if curr_term == &term {
//                     if let Some(postings) = &it.current_postings {
//                         posting_lists.push(postings.clone());
//                     }
//                     it.next()?;
//                 }
//             }
//         }

//         let mut final_merged = Vec::new();
//         for postings in posting_lists {
//             final_merged = merge_postings(&final_merged, &postings);
//         }

//         let df = get_document_frequency(&final_merged);
//         for posting in &final_merged {
//             let tf = get_term_frequency(posting);
//             let v = doc_lengths.get_mut(&posting.doc_id);
//             if v.is_some() {
//                 let vec = v.unwrap();
//                 vec.push(tf * df);
//             }
//         }

//         in_memory_dict.add_term(&term, posting_offset, final_merged.len() as u32);
//         writer.write_all(&(term.len() as u32).to_le_bytes())?;
//         writer.write_all(term.as_bytes())?;
//         let encoded_posting_list = vb_encode_posting_list(&final_merged);
//         writer.write_all(&(encoded_posting_list.len() as u32).to_le_bytes())?;
//         writer.write_all(&encoded_posting_list)?;
//         posting_offset =
//             posting_offset + 8 + (term.as_bytes().len() + encoded_posting_list.len()) as u32;
//     }
//     for doc_id in 1..doc_lengths.len() + 1 {
//         let mut doc_length: f32 = 0.0;
//         if let Some(tf_idfs) = doc_lengths.get(&(doc_id as u32)) {
//             for tf_idf in tf_idfs {
//                 doc_length = doc_length + (tf_idf * tf_idf);
//             }
//         }
//         doc_lengths_final.push(doc_length.sqrt());
//     }
//     let no_of_term_bytes = no_of_terms.to_le_bytes();
//     writer.seek(io::SeekFrom::Start(0))?;
//     writer.write_all(&no_of_term_bytes)?;
//     writer.flush()?;
//     Ok(Some(MergeIndexFileResult {
//         in_memory_dict,
//         bk_tree,
//         doc_lengths: doc_lengths_final,
//     }))
// }

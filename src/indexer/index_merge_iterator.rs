use std::{
    fs::File,
    io::{self, Read, Seek},
};

use crate::{dictionary::Posting, indexer::helper::vb_decode_posting_list};

pub struct IndexMergeIterator {
    no_of_terms: u32,
    file: File,
    current_term_no: u32,
    pub current_term: Option<String>,
    pub current_postings: Option<Vec<Posting>>,
    current_offset: u32,
}

impl IndexMergeIterator {
    pub fn new(file: File) -> IndexMergeIterator {
        IndexMergeIterator {
            file,
            no_of_terms: 0,
            current_term_no: 0,
            current_term: None,
            current_postings: None,
            current_offset: 0,
        }
    }

    pub fn get_current_term(&mut self) -> u32 {
        self.current_term_no
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dictionary::Posting, indexer::helper::vb_encode_posting_list};
    use std::io::{Seek, Write};
    use tempfile::NamedTempFile;

    // Helper function to create a test index file
    fn create_test_index_file(terms: Vec<(&str, Vec<Posting>)>) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();

        // Write number of terms
        file.write_all(&(terms.len() as u32).to_le_bytes()).unwrap();

        // Write each term and its postings
        for (term, postings) in terms {
            file.write_all(&(term.len() as u32).to_le_bytes()).unwrap();
            file.write_all(term.as_bytes()).unwrap();
            let encoded_posting_list = vb_encode_posting_list(&postings);
            file.write_all(&(encoded_posting_list.len() as u32).to_le_bytes())
                .unwrap();
            file.write_all(&encoded_posting_list).unwrap();
        }

        file.flush().unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        file
    }

    #[test]
    fn test_init_and_single_term() {
        let postings = vec![
            Posting {
                doc_id: 1,
                positions: vec![5, 10, 15],
            },
            Posting {
                doc_id: 3,
                positions: vec![2, 8],
            },
        ];

        let temp_file = create_test_index_file(vec![("apple", postings.clone())]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.no_of_terms, 1);
        assert_eq!(iterator.current_term_no, 1);
        assert_eq!(iterator.current_term, Some("apple".to_string()));
        assert_eq!(iterator.current_postings.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_iterate_multiple_terms() {
        let postings1 = vec![Posting {
            doc_id: 1,
            positions: vec![3, 7],
        }];
        let postings2 = vec![Posting {
            doc_id: 2,
            positions: vec![1],
        }];
        let postings3 = vec![Posting {
            doc_id: 5,
            positions: vec![7, 14, 21],
        }];

        let temp_file = create_test_index_file(vec![
            ("apple", postings1),
            ("banana", postings2),
            ("cherry", postings3),
        ]);

        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        // First term (automatically loaded by init)
        assert_eq!(iterator.current_term, Some("apple".to_string()));
        assert_eq!(iterator.get_current_term(), 1);

        // Second term
        assert!(iterator.next().unwrap());
        assert_eq!(iterator.current_term, Some("banana".to_string()));
        assert_eq!(iterator.get_current_term(), 2);

        // Third term
        assert!(iterator.next().unwrap());
        assert_eq!(iterator.current_term, Some("cherry".to_string()));
        assert_eq!(iterator.get_current_term(), 3);

        // No more terms
        assert!(!iterator.next().unwrap());
        assert!(iterator.current_term.is_none());
        assert!(iterator.current_postings.is_none());
    }

    #[test]
    fn test_empty_index() {
        let temp_file = create_test_index_file(vec![]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.no_of_terms, 0);
        assert!(iterator.current_term.is_none());
        assert!(iterator.current_postings.is_none());
    }

    #[test]
    fn test_term_with_empty_postings() {
        let postings = vec![];

        let temp_file = create_test_index_file(vec![("empty", postings)]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.current_term, Some("empty".to_string()));
        assert_eq!(iterator.current_postings.as_ref().unwrap().len(), 0);
    }

    #[test]
    fn test_term_with_multiple_postings() {
        let postings = vec![
            Posting {
                doc_id: 1,
                positions: vec![10, 20],
            },
            Posting {
                doc_id: 5,
                positions: vec![3, 7, 11],
            },
            Posting {
                doc_id: 10,
                positions: vec![1],
            },
            Posting {
                doc_id: 15,
                positions: vec![20, 40, 60, 80],
            },
        ];

        let temp_file = create_test_index_file(vec![("test", postings.clone())]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.current_term, Some("test".to_string()));
        let current_postings = iterator.current_postings.as_ref().unwrap();
        assert_eq!(current_postings.len(), 4);
        assert_eq!(current_postings[0].doc_id, 1);
        assert_eq!(current_postings[0].positions, vec![10, 20]);
        assert_eq!(current_postings[3].doc_id, 15);
        assert_eq!(current_postings[3].positions, vec![20, 40, 60, 80]);
    }

    #[test]
    fn test_unicode_term() {
        let postings = vec![Posting {
            doc_id: 1,
            positions: vec![1],
        }];

        let temp_file = create_test_index_file(vec![("café", postings)]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.current_term, Some("café".to_string()));
    }

    #[test]
    fn test_long_term_name() {
        let long_term = "a".repeat(1000);
        let postings = vec![Posting {
            doc_id: 1,
            positions: vec![1],
        }];

        let temp_file = create_test_index_file(vec![(&long_term, postings)]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.current_term, Some(long_term));
    }

    #[test]
    fn test_offset_tracking() {
        let postings = vec![Posting {
            doc_id: 1,
            positions: vec![2, 4],
        }];

        let temp_file =
            create_test_index_file(vec![("a", postings.clone()), ("b", postings.clone())]);

        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        let offset_after_first = iterator.current_offset;
        assert!(offset_after_first > 4); // Should be past the header

        iterator.next().unwrap();
        assert!(iterator.current_offset > offset_after_first);
    }

    #[test]
    fn test_next_beyond_end() {
        let postings = vec![Posting {
            doc_id: 1,
            positions: vec![1],
        }];

        let temp_file = create_test_index_file(vec![("single", postings)]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        // Try to iterate past the end multiple times
        assert!(!iterator.next().unwrap());
        assert!(!iterator.next().unwrap());
        assert!(!iterator.next().unwrap());

        // Should remain None
        assert!(iterator.current_term.is_none());
        assert!(iterator.current_postings.is_none());
    }

    #[test]
    fn test_posting_with_many_positions() {
        let postings = vec![Posting {
            doc_id: 42,
            positions: vec![1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
        }];

        let temp_file = create_test_index_file(vec![("frequent", postings.clone())]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.current_term, Some("frequent".to_string()));
        let current_postings = iterator.current_postings.as_ref().unwrap();
        assert_eq!(current_postings.len(), 1);
        assert_eq!(current_postings[0].doc_id, 42);
        assert_eq!(current_postings[0].positions.len(), 11);
        assert_eq!(current_postings[0].positions[0], 1);
        assert_eq!(current_postings[0].positions[10], 50);
    }

    #[test]
    fn test_posting_with_no_positions() {
        let postings = vec![Posting {
            doc_id: 10,
            positions: vec![],
        }];

        let temp_file = create_test_index_file(vec![("rare", postings)]);
        let file = temp_file.reopen().unwrap();
        let mut iterator = IndexMergeIterator::new(file);

        iterator.init().unwrap();

        assert_eq!(iterator.current_term, Some("rare".to_string()));
        let current_postings = iterator.current_postings.as_ref().unwrap();
        assert_eq!(current_postings.len(), 1);
        assert_eq!(current_postings[0].doc_id, 10);
        assert_eq!(current_postings[0].positions.len(), 0);
    }
}

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
};

use crate::{
  dictionary::{Dictionary, Posting, Term}, indexer::helper::{vb_decode_posting_list, vb_encode_posting_list}
};

fn read_block_from_disk(filename: &str) -> Result<Dictionary, std::io::Error> {
    let file = File::open(filename)?;
    let mut reader = BufReader::new(file);

    // Read total number of terms
    let mut term_count_bytes = [0u8; 4];
    reader.read_exact(&mut term_count_bytes)?;
    let term_count = u32::from_le_bytes(term_count_bytes) as usize;

    let mut dict = Dictionary::new();
    for _ in 0..term_count {
        match read_term_from_disk(&mut reader) {
            Ok((term, posting_list)) => {
                dict.add_term_posting(&term, posting_list);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // End of file
            Err(e) => return Err(e),
        }
    }

    Ok(dict)
}

pub fn write_block_to_disk(
    filename: &str,
    sorted_terms: &Vec<String>,
    dict: &Dictionary,
) -> Result<(), std::io::Error> {
    let file = File::create(filename)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&(sorted_terms.len() as u32).to_le_bytes())?;
    for term in sorted_terms {
        if let Some(posting_list) = dict.get_postings(term) {
            write_term_to_disk(&mut writer, term, &posting_list)?;
        }
    }

    writer.flush()?;
    return Ok(());
}

fn read_term_from_disk(
    reader: &mut BufReader<File>,
) -> Result<(String, Vec<Posting>), std::io::Error> {
    // Read term length
    let mut term_len_bytes = [0u8; 4];
    reader.read_exact(&mut term_len_bytes)?;
    let term_len = u32::from_le_bytes(term_len_bytes) as usize;

    // Read term
    let mut term_bytes = vec![0u8; term_len];
    reader.read_exact(&mut term_bytes)?;
    let term = String::from_utf8(term_bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Read encoded posting list length
    let mut posting_list_len_bytes = [0u8; 4];
    reader.read_exact(&mut posting_list_len_bytes)?;
    let posting_list_len = u32::from_le_bytes(posting_list_len_bytes) as usize;

    // Read encoded posting list
    let mut encoded_posting_list = vec![0u8; posting_list_len];
    reader.read_exact(&mut encoded_posting_list)?;

    // Decode posting list
    let posting_list = vb_decode_posting_list(&encoded_posting_list);

    Ok((term, posting_list))
}

fn write_term_to_disk(
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

pub fn single_pass_in_memory_indexing(token_stream: Vec<Term>) -> Result<(), std::io::Error> {
    let mut dict = Dictionary::new();
    for term in token_stream {
        let does_term_already_exist = dict.does_term_already_exist(&term.term);
        if !does_term_already_exist {
            dict.add_term(&term.term);
        }
        dict.append_to_term(&term.term, term.posting);
    }

    let sorted_terms = dict.sort_terms();
    write_block_to_disk("", &sorted_terms, &dict)?;
    Ok(())
}

#[cfg(test)]
mod posting_list_encode_decode_tests {
    use super::*;

    #[test]
    fn test_empty_posting_list() {
        let original: Vec<Posting> = Vec::new();
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
        assert_eq!(encoded.len(), 0);
    }

    #[test]
    fn test_single_posting_single_position() {
        let original = vec![Posting {
            doc_id: 42,
            positions: vec![10],
        }];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_single_posting_multiple_positions() {
        let original = vec![Posting {
            doc_id: 100,
            positions: vec![5, 12, 25, 30],
        }];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_single_posting_empty_positions() {
        let original = vec![Posting {
            doc_id: 15,
            positions: vec![],
        }];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_multiple_postings_ascending_doc_ids() {
        let original = vec![
            Posting {
                doc_id: 10,
                positions: vec![1, 5],
            },
            Posting {
                doc_id: 25,
                positions: vec![2, 8, 12],
            },
            Posting {
                doc_id: 50,
                positions: vec![3],
            },
            Posting {
                doc_id: 100,
                positions: vec![1, 4, 7, 10],
            },
        ];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_large_doc_ids() {
        let original = vec![
            Posting {
                doc_id: 1000000,
                positions: vec![1],
            },
            Posting {
                doc_id: 2000000,
                positions: vec![5, 10],
            },
            Posting {
                doc_id: 4294967295,
                positions: vec![2],
            }, // Max u32
        ];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_large_position_values() {
        let original = vec![Posting {
            doc_id: 1,
            positions: vec![1000000, 2000000, 4294967295],
        }];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_many_positions() {
        let positions: Vec<u32> = (1..=1000).collect();
        let original = vec![Posting {
            doc_id: 42,
            positions,
        }];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_consecutive_doc_ids() {
        let original = vec![
            Posting {
                doc_id: 1,
                positions: vec![1],
            },
            Posting {
                doc_id: 2,
                positions: vec![2],
            },
            Posting {
                doc_id: 3,
                positions: vec![3],
            },
            Posting {
                doc_id: 4,
                positions: vec![4],
            },
            Posting {
                doc_id: 5,
                positions: vec![5],
            },
        ];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_mixed_position_counts() {
        let original = vec![
            Posting {
                doc_id: 5,
                positions: vec![],
            },
            Posting {
                doc_id: 10,
                positions: vec![1],
            },
            Posting {
                doc_id: 20,
                positions: vec![1, 2],
            },
            Posting {
                doc_id: 30,
                positions: vec![1, 2, 3],
            },
            Posting {
                doc_id: 40,
                positions: vec![],
            },
            Posting {
                doc_id: 50,
                positions: vec![10, 20, 30, 40, 50],
            },
        ];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_large_doc_id_differences() {
        let original = vec![
            Posting {
                doc_id: 1,
                positions: vec![1],
            },
            Posting {
                doc_id: 1000000,
                positions: vec![2],
            },
            Posting {
                doc_id: 2000000,
                positions: vec![3],
            },
        ];
        let encoded = vb_encode_posting_list(&original);
        let decoded = vb_decode_posting_list(&encoded);

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_empty_bytes() {
        let empty_bytes: Vec<u8> = Vec::new();
        let decoded = vb_decode_posting_list(&empty_bytes);

        assert_eq!(decoded, Vec::<Posting>::new());
    }
}


#[cfg(test)]
mod write_and_read_term_tests {
    use super::*;
    use std::fs::{File, remove_file};
    use std::io::{BufReader, BufWriter};
    use std::path::PathBuf;

    fn create_test_file_path(test_name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("test_{}_{}.bin", test_name, std::process::id()));
        path
    }

    fn cleanup_test_file(path: &PathBuf) {
        let _ = remove_file(path); // Ignore errors if file doesn't exist
    }

    #[test]
    fn test_write_read_basic_term() {
        let file_path = create_test_file_path("basic_term");

        // Original data
        let original_term = "hello";
        let original_posting_list = vec![
            Posting {
                doc_id: 1,
                positions: vec![10, 25, 50],
            },
            Posting {
                doc_id: 3,
                positions: vec![5, 15],
            },
        ];

        // Write to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, original_term, &original_posting_list);
            assert!(result.is_ok());
        } // Writer is dropped and flushed here

        // Read from file
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_empty_term() {
        let file_path = create_test_file_path("empty_term");

        // Original data
        let original_term = "";
        let original_posting_list = vec![Posting {
            doc_id: 42,
            positions: vec![100],
        }];

        // Write to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, original_term, &original_posting_list);
            assert!(result.is_ok());
        }

        // Read from file
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_empty_posting_list() {
        let file_path = create_test_file_path("empty_posting_list");

        // Original data
        let original_term = "empty_postings";
        let original_posting_list = vec![];

        // Write to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, original_term, &original_posting_list);
            assert!(result.is_ok());
        }

        // Read from file
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_unicode_term() {
        let file_path = create_test_file_path("unicode_term");

        // Original data with Unicode
        let original_term = "café";
        let original_posting_list = vec![
            Posting {
                doc_id: 1,
                positions: vec![1, 10],
            },
            Posting {
                doc_id: 5,
                positions: vec![20, 30, 40],
            },
        ];

        // Write to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, original_term, &original_posting_list);
            assert!(result.is_ok());
        }

        // Read from file
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_long_term() {
        let file_path = create_test_file_path("long_term");

        let original_term = "a".repeat(1000);
        let original_posting_list = vec![Posting {
            doc_id: 999,
            positions: vec![1, 2, 3, 4, 5],
        }];

        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, &original_term, &original_posting_list);
            assert!(result.is_ok());
        }

        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_large_posting_list() {
        let file_path = create_test_file_path("large_posting_list");

        // Original data with large posting list
        let original_term = "popular";
        let mut original_posting_list = Vec::new();
        for i in 1..=100 {
            original_posting_list.push(Posting {
                doc_id: i,
                positions: (0..i % 5 + 1).map(|j| i * 10 + j).collect(),
            });
        }

        // Write to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, original_term, &original_posting_list);
            assert!(result.is_ok());
        }

        // Read from file
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_single_position() {
        let file_path = create_test_file_path("single_position");

        // Original data with single position per posting
        let original_term = "rare";
        let original_posting_list = vec![
            Posting {
                doc_id: 1,
                positions: vec![42],
            },
            Posting {
                doc_id: 100,
                positions: vec![999],
            },
            Posting {
                doc_id: 555,
                positions: vec![0],
            },
        ];

        // Write to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, original_term, &original_posting_list);
            assert!(result.is_ok());
        }

        // Read from file
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_multiple_terms_sequentially() {
        let file_path: PathBuf = create_test_file_path("multiple_terms");

        // Original data - multiple terms
        let terms_and_postings = vec![
            (
                "first",
                vec![Posting {
                    doc_id: 1,
                    positions: vec![10],
                }],
            ),
            (
                "second",
                vec![Posting {
                    doc_id: 2,
                    positions: vec![20, 25],
                }],
            ),
            (
                "third",
                vec![Posting {
                    doc_id: 3,
                    positions: vec![30, 35, 40],
                }],
            ),
        ];

        // Write all terms to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            for (term, posting_list) in &terms_and_postings {
                let result = write_term_to_disk(&mut writer, term, posting_list);
                assert!(result.is_ok());
            }
        }

        // Read all terms from file and verify
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);

            for (original_term, original_posting_list) in &terms_and_postings {
                let result = read_term_from_disk(&mut reader);
                assert!(result.is_ok());

                let (read_term, read_posting_list) = result.unwrap();
                assert_eq!(&read_term, original_term);
                assert_eq!(&read_posting_list, original_posting_list);
            }

            // Try to read one more - should fail because we've reached EOF
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_err());
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_empty_positions() {
        let file_path = create_test_file_path("empty_positions");

        // Original data with empty positions (edge case)
        let original_term = "no_positions";
        let original_posting_list = vec![
            Posting {
                doc_id: 1,
                positions: vec![],
            },
            Posting {
                doc_id: 2,
                positions: vec![],
            },
        ];

        // Write to file
        {
            let file = File::create(&file_path).unwrap();
            let mut writer = BufWriter::new(file);
            let result = write_term_to_disk(&mut writer, original_term, &original_posting_list);
            assert!(result.is_ok());
        }

        // Read from file
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_ok());

            let (read_term, read_posting_list) = result.unwrap();
            assert_eq!(read_term, original_term);
            assert_eq!(read_posting_list, original_posting_list);
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_read_from_empty_file_should_fail() {
        let file_path = create_test_file_path("empty_file");

        // Create empty file
        File::create(&file_path).unwrap();

        // Try to read from empty file - should fail
        {
            let file = File::open(&file_path).unwrap();
            let mut reader = BufReader::new(file);
            let result = read_term_from_disk(&mut reader);
            assert!(result.is_err());
        }

        // Cleanup
        cleanup_test_file(&file_path);
    }
}


#[cfg(test)]
mod read_write_block_tests {
    use super::*;
    use std::fmt;
    use std::fs::remove_file;
    use std::path::PathBuf;

    fn create_test_file_path(test_name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "test_block_{}_{}.bin",
            test_name,
            std::process::id()
        ));
        path
    }

    fn cleanup_test_file(path: &PathBuf) {
        let _ = remove_file(path); // Ignore errors if file doesn't exist
    }

    fn dictionaries_equal(dict1: &Dictionary, dict2: &Dictionary) -> bool {
        let terms1 = dict1.sort_terms();
        let terms2 = dict2.sort_terms();

        if terms1 != terms2 {
            return false;
        }

        for term in &terms1 {
            let postings1 = dict1.get_postings(term);
            let postings2 = dict2.get_postings(term);

            match (postings1, postings2) {
                (Some(p1), Some(p2)) => {
                    if p1 != p2 {
                        return false;
                    }
                }
                (None, None) => continue,
                _ => return false,
            }
        }

        true
    }

    #[test]
    fn test_write_read_basic_dictionary() {
        let file_path = create_test_file_path("basic_dict");
        let filename = file_path.to_str().unwrap();

        // Create original dictionary
        let mut original_dict = Dictionary::new();
        original_dict.add_term_posting(
            "apple",
            vec![
                Posting {
                    doc_id: 1,
                    positions: vec![10, 20],
                },
                Posting {
                    doc_id: 3,
                    positions: vec![5],
                },
            ],
        );
        original_dict.add_term_posting(
            "banana",
            vec![Posting {
                doc_id: 2,
                positions: vec![15, 25, 35],
            }],
        );
        original_dict.add_term_posting(
            "cherry",
            vec![
                Posting {
                    doc_id: 1,
                    positions: vec![30],
                },
                Posting {
                    doc_id: 4,
                    positions: vec![40, 50],
                },
            ],
        );

        let sorted_terms = original_dict.sort_terms();

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_empty_dictionary() {
        let file_path = create_test_file_path("empty_dict");
        let filename = file_path.to_str().unwrap();

        // Create empty dictionary
        let original_dict = Dictionary::new();
        let sorted_terms = original_dict.sort_terms();

        // Write empty dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_single_term_dictionary() {
        let file_path = create_test_file_path("single_term");
        let filename = file_path.to_str().unwrap();

        // Create dictionary with single term
        let mut original_dict = Dictionary::new();
        original_dict.add_term_posting(
            "hello",
            vec![Posting {
                doc_id: 42,
                positions: vec![100, 200, 300],
            }],
        );

        let sorted_terms = original_dict.sort_terms();

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_unicode_terms() {
        let file_path = create_test_file_path("unicode_terms");
        let filename = file_path.to_str().unwrap();

        // Create dictionary with Unicode terms
        let mut original_dict = Dictionary::new();
        original_dict.add_term_posting(
            "café",
            vec![Posting {
                doc_id: 1,
                positions: vec![0],
            }],
        );
        original_dict.add_term_posting(
            "a",
            vec![Posting {
                doc_id: 2,
                positions: vec![10, 20],
            }],
        );
        original_dict.add_term_posting(
            "csavcds",
            vec![Posting {
                doc_id: 3,
                positions: vec![5, 15, 25],
            }],
        );

        let sorted_terms = original_dict.sort_terms();

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_large_dictionary() {
        let file_path = create_test_file_path("large_dict");
        let filename = file_path.to_str().unwrap();

        // Create large dictionary
        let mut original_dict = Dictionary::new();
        for i in 0..100 {
            let term = format!("term_{:03}", i);
            let mut postings = Vec::new();
            for j in 1..=i % 5 + 1 {
                postings.push(Posting {
                    doc_id: j * 10 + i,
                    positions: (0..j).map(|k| k * 100 + i * 10).collect(),
                });
            }
            original_dict.add_term_posting(&term, postings);
        }

        let sorted_terms = original_dict.sort_terms();

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_terms_with_empty_posting_lists() {
        let file_path = create_test_file_path("empty_postings");
        let filename = file_path.to_str().unwrap();

        // Create dictionary with terms that have empty posting lists
        let mut original_dict = Dictionary::new();
        original_dict.add_term_posting("empty1", vec![]);
        original_dict.add_term_posting(
            "normal",
            vec![Posting {
                doc_id: 1,
                positions: vec![10],
            }],
        );
        original_dict.add_term_posting("empty2", vec![]);

        let sorted_terms = original_dict.sort_terms();

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_terms_with_empty_positions() {
        let file_path = create_test_file_path("empty_positions");
        let filename = file_path.to_str().unwrap();

        // Create dictionary with postings that have empty positions
        let mut original_dict = Dictionary::new();
        original_dict.add_term_posting(
            "no_positions",
            vec![
                Posting {
                    doc_id: 1,
                    positions: vec![],
                },
                Posting {
                    doc_id: 2,
                    positions: vec![],
                },
            ],
        );
        original_dict.add_term_posting(
            "with_positions",
            vec![Posting {
                doc_id: 3,
                positions: vec![10, 20],
            }],
        );

        let sorted_terms = original_dict.sort_terms();

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_sorted_terms_order() {
        let file_path = create_test_file_path("sorted_order");
        let filename = file_path.to_str().unwrap();

        // Create dictionary with terms in non-alphabetical order
        let mut original_dict = Dictionary::new();
        original_dict.add_term_posting(
            "zebra",
            vec![Posting {
                doc_id: 1,
                positions: vec![1],
            }],
        );
        original_dict.add_term_posting(
            "apple",
            vec![Posting {
                doc_id: 2,
                positions: vec![2],
            }],
        );
        original_dict.add_term_posting(
            "banana",
            vec![Posting {
                doc_id: 3,
                positions: vec![3],
            }],
        );

        // Get sorted terms (should be alphabetical)
        let sorted_terms = original_dict.sort_terms();
        assert_eq!(sorted_terms, vec!["apple", "banana", "zebra"]);

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_read_from_nonexistent_file() {
        let file_path = create_test_file_path("nonexistent");
        let filename = file_path.to_str().unwrap();

        // Try to read from non-existent file - should fail
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_err());

        // Cleanup (file doesn't exist, but cleanup is safe)
        cleanup_test_file(&file_path);
    }

    #[test]
    fn test_write_read_very_long_terms() {
        let file_path = create_test_file_path("long_terms");
        let filename = file_path.to_str().unwrap();

        // Create dictionary with very long terms
        let mut original_dict = Dictionary::new();
        let long_term1 = "a".repeat(1000);
        let long_term2 = "b".repeat(500);

        original_dict.add_term_posting(
            &long_term1,
            vec![Posting {
                doc_id: 1,
                positions: vec![10],
            }],
        );
        original_dict.add_term_posting(
            &long_term2,
            vec![Posting {
                doc_id: 2,
                positions: vec![20, 30],
            }],
        );

        let sorted_terms = original_dict.sort_terms();

        // Write dictionary to disk
        let write_result = write_block_to_disk(filename, &sorted_terms, &original_dict);
        assert!(write_result.is_ok());

        // Read dictionary from disk
        let read_result = read_block_from_disk(filename);
        assert!(read_result.is_ok());

        let read_dict = read_result.unwrap();

        // Compare dictionaries
        assert!(dictionaries_equal(&read_dict, &original_dict));

        // Cleanup
        cleanup_test_file(&file_path);
    }
}

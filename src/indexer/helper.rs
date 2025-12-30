use std::{
    fs::File,
    io::{self, BufReader},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
        mpsc,
    },
};

use once_cell::sync::Lazy;
use regex::Regex;
use rustc_hash::FxHashMap;
use zstd::Decoder;

use crate::{
    indexer::types::WikiArticle,
    parser::parser::Parser,
    utils::{posting::Posting, term::Term},
};

static TAG_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]*>").unwrap());

pub(crate) fn extract_plaintext(text: &[Vec<String>]) -> String {
    let total_len: usize = text
        .iter()
        .map(|para| para.iter().map(|s| s.len()).sum::<usize>())
        .sum();
    let mut result = String::with_capacity(total_len + text.len() * 2);
    for (i, paragraph) in text.iter().enumerate() {
        if i > 0 {
            result.push_str("\n\n");
        }
        for sentence in paragraph {
            result.push_str(sentence);
        }
    }
    TAG_REGEX.replace_all(&result, "").into_owned()
}

pub(crate) fn read_zstd_file(
    path: &Path,
    tx: &mpsc::SyncSender<Vec<Term>>,
    doc_id: &Arc<AtomicU32>,
    doc_lengths: &Arc<Mutex<Vec<u32>>>,
    doc_urls: &Arc<Mutex<Vec<String>>>,
    doc_names: &Arc<Mutex<Vec<String>>>,
    search_tokenizer: &Parser,
) -> io::Result<()> {
    let file = File::open(path)?;
    let decoder = Decoder::new(file)?;
    let reader = BufReader::new(decoder);

    let stream = serde_json::Deserializer::from_reader(reader).into_iter::<WikiArticle>();
    let mut terms = Vec::with_capacity(50000);

    let mut local_lengths = Vec::with_capacity(400);
    let mut local_names = Vec::with_capacity(400);
    let mut local_urls = Vec::with_capacity(400);
    let mut local_doc_index = 0u32;
    for result in stream {
        match result {
            Ok(article) => {
                let mut doc_postings: FxHashMap<&str, Vec<u32>> =
                    FxHashMap::with_capacity_and_hasher(400, Default::default());

                let plain_text = extract_plaintext(&article.text);
                let tokens = search_tokenizer.tokenize(&plain_text);
                if tokens.len() == 0 {
                    continue;
                }
                local_lengths.push(tokens.len() as u32);
                local_names.push(article.title);
                local_urls.push(article.url);
                for token in &tokens {
                    doc_postings
                        .entry(&token.word)
                        .or_insert_with(Vec::new)
                        .push(token.position);
                }
                for (key, value) in doc_postings.drain() {
                    let term = Term {
                        posting: Posting::new(local_doc_index, value),
                        term: key.to_string(),
                    };
                    terms.push(term);
                }
                local_doc_index += 1;
            }
            Err(e) => {
                eprintln!("Error parsing: {}", e);
            }
        }
    }

    let start_doc_id = {
        let mut lengths = doc_lengths.lock().unwrap();
        let mut names = doc_names.lock().unwrap();
        let mut urls = doc_urls.lock().unwrap();

        let start_id = doc_id.fetch_add(local_lengths.len() as u32, Ordering::SeqCst);

        lengths.append(&mut local_lengths);
        names.append(&mut local_names);
        urls.append(&mut local_urls);

        start_id
    };

    for term in &mut terms {
        term.posting.doc_id = start_doc_id + term.posting.doc_id + 1;
    }

    tx.send(terms).unwrap();

    Ok(())
}

pub(crate) fn vb_decode_posting_list(encoded_bytes: &[u8]) -> Vec<Posting> {
    let mut posting_list: Vec<Posting> = Vec::new();
    let mut offset = 0;

    while offset < encoded_bytes.len() {
        let doc_id = u32::from_le_bytes(encoded_bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let no_of_positions =
            u32::from_le_bytes(encoded_bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let mut positions = Vec::with_capacity(no_of_positions as usize);
        for _ in 0..no_of_positions {
            let position =
                u32::from_le_bytes(encoded_bytes[offset..offset + 4].try_into().unwrap());
            positions.push(position);
            offset += 4;
        }
        posting_list.push(Posting { doc_id, positions });
    }

    posting_list
}

pub(crate) fn vb_encode_posting_list(posting_list: &Vec<Posting>) -> Vec<u8> {
    let mut posting_list_bytes: Vec<u8> = Vec::<u8>::with_capacity(200);
    // posting_list.sort_by(|a, b| a.doc_id.cmp(&b.doc_id));
    let mut indices: Vec<usize> = (0..posting_list.len()).collect();
    indices.sort_unstable_by_key(|&i| posting_list[i].doc_id);

    for &idx in &indices {
        let posting = &posting_list[idx];
        posting_list_bytes.extend(posting.doc_id.to_le_bytes());
        posting_list_bytes.extend((posting.positions.len() as u32).to_le_bytes());
        for position in &posting.positions {
            posting_list_bytes.extend(position.to_le_bytes());
        }
        // if last_doc_id == 0 {
        //     let mut posting_bytes = vb_encode(&posting.doc_id);
        //     let mut position_bytes = vb_encode_positions(&posting.positions);
        //     posting_list_bytes.append(&mut posting_bytes);
        //     let positions_length: u16 = position_bytes.len() as u16;
        //     let mut length_bytes: Vec<u8> = positions_length.to_le_bytes().to_vec();
        //     posting_list_bytes.append(&mut length_bytes);
        //     posting_list_bytes.append(&mut position_bytes);
        // } else {
        //     let doc_id_difference = posting.doc_id - last_doc_id;
        //     let mut posting_bytes = vb_encode(&doc_id_difference);
        //     let mut position_bytes = vb_encode_positions(&posting.positions);
        //     posting_list_bytes.append(&mut posting_bytes);
        //     let positions_length: u16 = position_bytes.len() as u16;
        //     let mut length_bytes: Vec<u8> = positions_length.to_le_bytes().to_vec();
        //     posting_list_bytes.append(&mut length_bytes);
        //     posting_list_bytes.append(&mut position_bytes);
        // }
        // last_doc_id = posting.doc_id
    }

    posting_list_bytes
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    pub doc_id: u32,
    pub positions: Vec<u32>,
}
impl Posting {
    pub fn new(doc_id: u32, positions: Vec<u32>) -> Self {
        Self { doc_id, positions }
    }
}

use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Eq, PartialEq)]
struct PostingWithSource {
    posting: Posting,
    list_idx: usize,
    pos_in_list: usize,
}

impl Ord for PostingWithSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap, compare by doc_id
        other.posting.doc_id.cmp(&self.posting.doc_id)
    }
}

impl PartialOrd for PostingWithSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn merge_all_postings(lists: &[Vec<Posting>]) -> Vec<Posting> {
    let total_size: usize = lists.iter().map(|l| l.len()).sum();
    let mut result = Vec::with_capacity(total_size);
    let mut heap = BinaryHeap::new();

    // Initialize heap with first element from each list
    for (idx, list) in lists.iter().enumerate() {
        if !list.is_empty() {
            heap.push(PostingWithSource {
                posting: list[0].clone(),
                list_idx: idx,
                pos_in_list: 0,
            });
        }
    }

    while let Some(PostingWithSource {
        posting,
        list_idx,
        pos_in_list,
    }) = heap.pop()
    {
        result.push(posting);

        // Add next element from same list
        let next_pos = pos_in_list + 1;
        if next_pos < lists[list_idx].len() {
            heap.push(PostingWithSource {
                posting: lists[list_idx][next_pos].clone(),
                list_idx,
                pos_in_list: next_pos,
            });
        }
    }

    result
}

pub fn merge_postings(p1: &[Posting], p2: &[Posting]) -> Vec<Posting> {
    let mut merged = Vec::with_capacity(p1.len() + p2.len());
    let mut i = 0;
    let mut j = 0;

    while i < p1.len() && j < p2.len() {
        if p1[i].doc_id < p2[j].doc_id {
            merged.push(p1[i].clone());
            i += 1;
        } else {
            merged.push(p2[j].clone());
            j += 1;
        }
    }

    if i < p1.len() {
        merged.extend_from_slice(&p1[i..]);
    }
    if j < p2.len() {
        merged.extend_from_slice(&p2[j..]);
    }

    merged
}

#[cfg(test)]
mod merge_postings_test {
    use super::*;

    #[test]
    fn test_merge_both_empty() {
        let p1: Vec<Posting> = vec![];
        let p2: Vec<Posting> = vec![];
        let result = merge_postings(&p1, &p2);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_merge_first_empty() {
        let p1: Vec<Posting> = vec![];
        let p2 = vec![
            Posting {
                doc_id: 1,
                positions: vec![10, 20],
            },
            Posting {
                doc_id: 3,
                positions: vec![30],
            },
        ];
        let result = merge_postings(&p1, &p2);
        let expected = vec![
            Posting {
                doc_id: 1,
                positions: vec![10, 20],
            },
            Posting {
                doc_id: 3,
                positions: vec![30],
            },
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_merge_interleaved() {
        let p1 = vec![
            Posting {
                doc_id: 1,
                positions: vec![10],
            },
            Posting {
                doc_id: 3,
                positions: vec![30, 35],
            },
            Posting {
                doc_id: 5,
                positions: vec![50],
            },
        ];
        let p2 = vec![
            Posting {
                doc_id: 2,
                positions: vec![20],
            },
            Posting {
                doc_id: 4,
                positions: vec![40],
            },
            Posting {
                doc_id: 6,
                positions: vec![60, 65],
            },
        ];
        let result = merge_postings(&p1, &p2);
        let expected = vec![
            Posting {
                doc_id: 1,
                positions: vec![10],
            },
            Posting {
                doc_id: 2,
                positions: vec![20],
            },
            Posting {
                doc_id: 3,
                positions: vec![30, 35],
            },
            Posting {
                doc_id: 4,
                positions: vec![40],
            },
            Posting {
                doc_id: 5,
                positions: vec![50],
            },
            Posting {
                doc_id: 6,
                positions: vec![60, 65],
            },
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_merge_first_all_smaller() {
        let p1 = vec![
            Posting {
                doc_id: 1,
                positions: vec![10],
            },
            Posting {
                doc_id: 2,
                positions: vec![20, 25],
            },
        ];
        let p2 = vec![
            Posting {
                doc_id: 5,
                positions: vec![50],
            },
            Posting {
                doc_id: 6,
                positions: vec![60],
            },
        ];
        let result = merge_postings(&p1, &p2);
        let expected = vec![
            Posting {
                doc_id: 1,
                positions: vec![10],
            },
            Posting {
                doc_id: 2,
                positions: vec![20, 25],
            },
            Posting {
                doc_id: 5,
                positions: vec![50],
            },
            Posting {
                doc_id: 6,
                positions: vec![60],
            },
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_merge_second_all_smaller() {
        let p1 = vec![
            Posting {
                doc_id: 5,
                positions: vec![50, 55],
            },
            Posting {
                doc_id: 6,
                positions: vec![60],
            },
        ];
        let p2 = vec![
            Posting {
                doc_id: 1,
                positions: vec![10],
            },
            Posting {
                doc_id: 2,
                positions: vec![20],
            },
        ];
        let result = merge_postings(&p1, &p2);
        let expected = vec![
            Posting {
                doc_id: 1,
                positions: vec![10],
            },
            Posting {
                doc_id: 2,
                positions: vec![20],
            },
            Posting {
                doc_id: 5,
                positions: vec![50, 55],
            },
            Posting {
                doc_id: 6,
                positions: vec![60],
            },
        ];
        assert_eq!(result, expected);
    }
}

// pub fn positional_intersect(p1: &Vec<Posting>, p2: &Vec<Posting>, k: u32) -> Vec<ProximityMatch> {
//     let mut intersection: Vec<ProximityMatch> = Vec::<ProximityMatch>::new();
//     let mut i = 0;
//     let mut j = 0;
//     while i < p1.len() && j < p2.len() {
//         if p1[i].doc_id == p2[j].doc_id {
//             let pp1 = &p1[i].positions;
//             let pp2 = &p2[j].positions;
//             let mut intersections = proximity_match(p1[i].doc_id, pp1, pp2, k);
//             intersection.append(&mut intersections);
//             i = i + 1;
//             j = j + 1;
//         } else {
//             if p1[i].doc_id < p2[j].doc_id {
//                 i = i + 1;
//             } else {
//                 j = j + 1;
//             }
//         }
//     }
//     return intersection;
// }

// #[cfg(test)]
// mod positional_intersect_tests {
//     use super::*;

//     #[test]
//     fn test_no_matching_docs() {
//         let p1 = vec![
//             Posting {
//                 doc_id: 1,
//                 positions: vec![10, 20],
//             },
//             Posting {
//                 doc_id: 3,
//                 positions: vec![30],
//             },
//         ];
//         let p2 = vec![
//             Posting {
//                 doc_id: 2,
//                 positions: vec![15],
//             },
//             Posting {
//                 doc_id: 4,
//                 positions: vec![40],
//             },
//         ];
//         let result = positional_intersect(&p1, &p2, 5);
//         assert_eq!(result, vec![]);
//     }

//     #[test]
//     fn test_empty_lists() {
//         let p1: Vec<Posting> = vec![];
//         let p2: Vec<Posting> = vec![];
//         let result = positional_intersect(&p1, &p2, 5);
//         assert_eq!(result, vec![]);
//     }

//     #[test]
//     fn test_one_empty_list() {
//         let p1 = vec![Posting {
//             doc_id: 1,
//             positions: vec![10],
//         }];
//         let p2: Vec<Posting> = vec![];
//         let result = positional_intersect(&p1, &p2, 5);
//         assert_eq!(result, vec![]);
//     }

//     #[test]
//     fn test_single_doc_match_within_proximity() {
//         let p1 = vec![Posting {
//             doc_id: 1,
//             positions: vec![10, 20],
//         }];
//         let p2 = vec![Posting {
//             doc_id: 1,
//             positions: vec![12, 25],
//         }];
//         let result = positional_intersect(&p1, &p2, 3);

//         assert_eq!(result.len(), 1);
//         assert_eq!(
//             result[0],
//             ProximityMatch {
//                 doc_id: 1,
//                 pp1: 10,
//                 pp2: 12
//             }
//         );
//     }

//     #[test]
//     fn test_single_doc_match_outside_proximity() {
//         let p1 = vec![Posting {
//             doc_id: 1,
//             positions: vec![10],
//         }];
//         let p2 = vec![Posting {
//             doc_id: 1,
//             positions: vec![20],
//         }];
//         let result = positional_intersect(&p1, &p2, 5);
//         // No match because diff=10 > k=5
//         assert_eq!(result.len(), 0);
//     }

//     #[test]
//     fn test_positions_not_in_order() {
//         let p1 = vec![Posting {
//             doc_id: 1,
//             positions: vec![20],
//         }];
//         let p2 = vec![Posting {
//             doc_id: 1,
//             positions: vec![10],
//         }];
//         let result = positional_intersect(&p1, &p2, 5);
//         // No match because pp2[10] is not > pp1[20]
//         assert_eq!(result.len(), 0);
//     }

//     #[test]
//     fn test_multiple_matches_same_doc() {
//         let p1 = vec![Posting {
//             doc_id: 1,
//             positions: vec![10, 15],
//         }];
//         let p2 = vec![Posting {
//             doc_id: 1,
//             positions: vec![12, 17, 20],
//         }];
//         let result = positional_intersect(&p1, &p2, 3);
//         // Expected matches:
//         // pp1=10: pp2=12 (diff=2 <= 3), pp2=20 (diff=10 > 3, no match)
//         // pp1=15: pp2=17 (diff=2 <= 3), pp2=20 (diff=5 > 3, no match)
//         assert_eq!(result.len(), 2);
//         assert!(result.contains(&ProximityMatch {
//             doc_id: 1,
//             pp1: 10,
//             pp2: 12
//         }));
//         assert!(result.contains(&ProximityMatch {
//             doc_id: 1,
//             pp1: 15,
//             pp2: 17
//         }));
//     }

//     #[test]
//     fn test_multiple_docs_some_match() {
//         let p1 = vec![
//             Posting {
//                 doc_id: 1,
//                 positions: vec![10],
//             },
//             Posting {
//                 doc_id: 3,
//                 positions: vec![30],
//             },
//             Posting {
//                 doc_id: 5,
//                 positions: vec![50],
//             },
//         ];
//         let p2 = vec![
//             Posting {
//                 doc_id: 2,
//                 positions: vec![20],
//             }, // no match
//             Posting {
//                 doc_id: 3,
//                 positions: vec![32],
//             }, // matches doc 3
//             Posting {
//                 doc_id: 6,
//                 positions: vec![60],
//             }, // no match
//         ];
//         let result = positional_intersect(&p1, &p2, 5);
//         // Only doc_id 3 should have matches
//         assert!(result.iter().all(|m| m.doc_id == 3));
//         assert_eq!(result.len(), 1);
//     }

//     #[test]
//     fn test_multiple_docs_multiple_matches() {
//         let p1 = vec![
//             Posting {
//                 doc_id: 1,
//                 positions: vec![10, 15],
//             },
//             Posting {
//                 doc_id: 2,
//                 positions: vec![20],
//             },
//         ];
//         let p2 = vec![
//             Posting {
//                 doc_id: 1,
//                 positions: vec![12],
//             },
//             Posting {
//                 doc_id: 2,
//                 positions: vec![22],
//             },
//         ];
//         let result = positional_intersect(&p1, &p2, 3);
//         // Should have matches for both doc_id 1 and 2
//         assert!(result.iter().any(|m| m.doc_id == 1));
//         assert!(result.iter().any(|m| m.doc_id == 2));
//     }

//     #[test]
//     fn test_sorted_input_assumption() {
//         // Test that function works with sorted posting lists
//         let p1 = vec![
//             Posting {
//                 doc_id: 1,
//                 positions: vec![10],
//             },
//             Posting {
//                 doc_id: 3,
//                 positions: vec![30],
//             },
//             Posting {
//                 doc_id: 5,
//                 positions: vec![50],
//             },
//         ];
//         let p2 = vec![
//             Posting {
//                 doc_id: 1,
//                 positions: vec![12],
//             },
//             Posting {
//                 doc_id: 4,
//                 positions: vec![40],
//             },
//             Posting {
//                 doc_id: 5,
//                 positions: vec![52],
//             },
//         ];
//         let result = positional_intersect(&p1, &p2, 5);
//         // Should find matches for doc_id 1 and 5
//         let doc_ids: Vec<u32> = result.iter().map(|m| m.doc_id).collect();
//         assert!(doc_ids.contains(&1));
//         assert!(doc_ids.contains(&5));
//         assert!(!doc_ids.contains(&3));
//         assert!(!doc_ids.contains(&4));
//     }
// }

// pub fn proximity_match(doc_id: u32, pp1: &Vec<u32>, pp2: &Vec<u32>, k: u32) -> Vec<ProximityMatch> {
//     let mut intersection: Vec<ProximityMatch> = Vec::<ProximityMatch>::new();
//     let mut i = 0;
//     while i < pp1.len() {
//         let mut j = 0;
//         let mut positions = Vec::<u32>::new();

//         while j < pp2.len() {
//             if pp2[j] > pp1[i] && pp1[i].abs_diff(pp2[j]) <= k {
//                 positions.push(pp2[j]);
//             }
//             j = j + 1;
//         }
//         if positions.len() > 0 {
//             for pp2 in &positions {
//                 intersection.push(ProximityMatch {
//                     doc_id: doc_id,
//                     pp1: pp1[i],
//                     pp2: *pp2,
//                 });
//             }
//         }
//         i = i + 1;
//     }
//     return intersection;
// }

// #[cfg(test)]
// mod proximity_match_tests {
//     use super::*;

//     #[test]
//     fn test_empty_position_lists() {
//         let pp1: Vec<u32> = vec![];
//         let pp2: Vec<u32> = vec![];
//         let result = proximity_match(1, &pp1, &pp2, 2);
//         assert_eq!(result.len(), 0);
//     }

//     #[test]
//     fn test_single_position_within_proximity() {
//         let pp1 = vec![5];
//         let pp2 = vec![7];
//         let result = proximity_match(2, &pp1, &pp2, 3);
//         assert_eq!(result.len(), 1);
//         assert_eq!(result[0].doc_id, 2);
//         assert_eq!(result[0].pp1, 5);
//         assert_eq!(result[0].pp2, 7);
//     }

//     #[test]
//     fn test_single_position_outside_proximity() {
//         let pp1 = vec![5];
//         let pp2 = vec![10];
//         let result = proximity_match(1, &pp1, &pp2, 4);
//         assert_eq!(result.len(), 0);
//     }

//     #[test]
//     fn test_multiple_positions_some_matches() {
//         let pp1 = vec![1, 5, 10];
//         let pp2 = vec![2, 6, 15];
//         let result = proximity_match(1, &pp1, &pp2, 2);

//         assert_eq!(result.len(), 2);

//         let matches: Vec<(u32, u32)> = result.iter().map(|m| (m.pp1, m.pp2)).collect();
//         assert!(matches.contains(&(1, 2)));
//         assert!(matches.contains(&(5, 6)));
//     }

//     #[test]
//     fn test_one_to_many_matches() {
//         let pp1 = vec![10];
//         let pp2 = vec![8, 9, 11, 12];
//         let result = proximity_match(3, &pp1, &pp2, 3);

//         assert_eq!(result.len(), 2);
//         for match_result in &result {
//             assert_eq!(match_result.doc_id, 3);
//             assert_eq!(match_result.pp1, 10);
//             assert!(vec![11, 12].contains(&match_result.pp2));
//         }
//     }

//     #[test]
//     fn test_many_to_one_matches() {
//         let pp1 = vec![8, 9, 11, 12];
//         let pp2 = vec![10];
//         let result = proximity_match(4, &pp1, &pp2, 3);

//         assert_eq!(result.len(), 2);
//         for match_result in &result {
//             assert_eq!(match_result.doc_id, 4);
//             assert!(vec![8, 9].contains(&match_result.pp1));
//             assert_eq!(match_result.pp2, 10);
//         }
//     }

//     #[test]
//     fn test_sorted_positions_optimal_case() {
//         let pp1 = vec![1, 5, 10, 15];
//         let pp2 = vec![2, 6, 11, 16];
//         let result = proximity_match(1, &pp1, &pp2, 2);

//         // Expected matches: (1,2), (5,6), (10,11), (15,16)
//         assert_eq!(result.len(), 4);

//         let mut matches: Vec<(u32, u32)> = result.iter().map(|m| (m.pp1, m.pp2)).collect();
//         matches.sort();
//         assert_eq!(matches, vec![(1, 2), (5, 6), (10, 11), (15, 16)]);
//     }

//     #[test]
//     fn test_large_proximity_value() {
//         let pp1 = vec![1, 100];
//         let pp2 = vec![50, 200];
//         let result = proximity_match(1, &pp1, &pp2, 100);

//         assert_eq!(result.len(), 2);

//         let matches: Vec<(u32, u32)> = result.iter().map(|m| (m.pp1, m.pp2)).collect();
//         assert!(matches.contains(&(1, 50)));
//         assert!(matches.contains(&(100, 200)));
//     }
// }

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::query_processor::algos::utils::{DocData, FloatDoc};
use crate::query_processor::term_iterator::TermIterator;

// fn main() {
//     let k = 3;

//     // Simulating a stream of documents with f32 scores
//     let docs = vec![
//         DocData { docid: 101, score: 0.10 },
//         DocData { docid: 102, score: 0.95 },
//         DocData { docid: 103, score: 0.05 },  // Low score
//         DocData { docid: 104, score: 1.00 },
//         DocData { docid: 105, score: 0.95 }, // Tie with doc 102
//         DocData { docid: 106, score: 0.50 },
//     ];

//     // Create the Min-Heap: BinaryHeap<Reverse<FloatDoc>>
// let mut pq: BinaryHeap<Reverse<FloatDoc>> = BinaryHeap::with_capacity(k);

//     for doc in docs {
//         let new_doc = FloatDoc(doc);

//         if pq.len() < k {
//             // Case 1: Fill the heap until it reaches size K
//             pq.push(Reverse(new_doc));
//         } else {
//             // Case 2: Heap is full. Check if new doc is better than the smallest.
//             // .peek() gives us the smallest item due to Reverse wrapper.
//             if let Some(Reverse(min_item)) = pq.peek() {
//                 if new_doc.0.score > min_item.0.score {
//                     pq.pop(); // Remove the smallest (worst)
//                     pq.push(Reverse(new_doc)); // Add the better new item
//                 }
//             }
//         }
//     }

//     // Output: Extract and sort the results
//     // pq.into_sorted_vec() returns Smallest -> Largest.
//     // We map it back to the original DocData and reverse the vector for Decreasing order (Highest Score first).
//     let mut top_k: Vec<DocData> = pq.into_sorted_vec()
//         .into_iter()
//         .map(|Reverse(FloatDoc(data))| data)
//         .collect();

//     top_k.reverse();

//     println!("⭐ Top {} documents by score (f32):", k);
//     for doc in top_k {
//         println!("  DocID: {}, Score: {:.2}", doc.docid, doc.score);
//     }
// }

pub fn sort_by_doc_id(term_iterators: &mut Vec<TermIterator>) {
    term_iterators.sort_by(|a, b| a.get_current_doc_id().cmp(&b.get_current_doc_id()));
}
pub fn swap_down(term_iterators: &mut Vec<TermIterator>, pivot: usize) {
    let mut temp = pivot;
    while (temp + 1 < term_iterators.len()
        && term_iterators[temp].get_current_doc_id()
            > term_iterators[temp + 1].get_current_doc_id())
    {
        term_iterators.swap(temp, temp + 1);
        temp += 1;
    }
}
pub fn wand(mut term_iterators: Vec<TermIterator>) -> Vec<u32> {
    let mut pq: BinaryHeap<Reverse<FloatDoc>> = BinaryHeap::with_capacity(20);
    let mut threshold = 0.0;
    sort_by_doc_id(&mut term_iterators);
    while true {
        let mut score: f32 = 0.0;
        let mut pivot = 0;
        while pivot < term_iterators.len() {
            // if term_iterators[pivot].get_current_doc_id()
            score += term_iterators[pivot].get_max_score();
            pivot += 1;
            if score > threshold {
                break;
            }
        }
        if score <= threshold {
            break;
        }
        let pivot_id = term_iterators[pivot].get_current_doc_id();
        if pivot_id == term_iterators[0].get_current_doc_id() {
            let mut score = 0.0;
            for i in 0..term_iterators.len() {
                if term_iterators[i].get_current_doc_id() != pivot_id {
                    break;
                }
                score += term_iterators[i].get_current_doc_score();
                term_iterators[i].next();
            }
            pq.push(Reverse(FloatDoc(DocData {
                docid: pivot_id,
                score,
            })));
            threshold = pq.peek().unwrap().0.0.score;
            sort_by_doc_id(&mut term_iterators);
        } else {
            while term_iterators[pivot].get_current_doc_id() == pivot_id {
                pivot -= 1;
            }
            term_iterators[pivot].advance(pivot_id);
            swap_down(&mut term_iterators, pivot);
        }
    }
    let mut doc_ids = Vec::new();
    while !pq.is_empty() {
        if let Some(doc) = pq.pop() {
            doc_ids.push(doc.0.0.docid);
        }
    }
    doc_ids
}

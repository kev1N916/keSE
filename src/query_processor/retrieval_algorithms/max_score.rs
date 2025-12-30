use std::{cmp::Reverse, collections::BinaryHeap, u32};

use crate::{
    query_processor::{
        retrieval_algorithms::utils::{DocData, FloatDoc},
        term_iterator::TermIterator,
    },
    scoring::bm_25::BM25Params,
};

pub fn max_score(
    mut term_iterators: Vec<TermIterator>,
    doc_lengths: &Box<[u32]>,
    average_doc_length: f32,
) -> Vec<(u32, f32)> {
    term_iterators.sort_by(|a, b| a.get_max_score().total_cmp(&b.get_max_score()));
    // for term_iterator in &term_iterators {
    //     println!("{}", term_iterator.get_max_score());
    // }
    let n = term_iterators.len();
    let mut ub = vec![0.0; term_iterators.len()];
    ub[0] = term_iterators[0].get_max_score();
    for i in 1..n {
        ub[i] = ub[i - 1] + term_iterators[i].get_max_score();
    }
    let mut pivot = 0;
    let mut threshold = 0.0;
    let max_size = 20;
    let mut pq: BinaryHeap<Reverse<FloatDoc>> = BinaryHeap::with_capacity(max_size);
    let mut current = u64::MAX;
    for term_iterator in &term_iterators {
        current = current.min(term_iterator.get_current_doc_id());
    }
    let params = BM25Params::default();
    while pivot < n && current != u64::MAX {
        let mut score = 0.0;
        let mut next = u64::MAX;

        for i in pivot..n {
            if term_iterators[i].get_current_doc_id() == current {
                score += term_iterators[i].get_current_doc_score(
                    &doc_lengths[current as usize - 1],
                    average_doc_length,
                    &params,
                    doc_lengths.len() as u32,
                );
                term_iterators[i].next();
            }
            if term_iterators[i].get_current_doc_id() < next {
                next = term_iterators[i].get_current_doc_id();
            }
        }

        for i in (0..pivot).rev() {
            if score + ub[i] <= threshold {
                break;
            }
            term_iterators[i].advance(current as u32);
            if term_iterators[i].get_current_doc_id() == current {
                score += term_iterators[i].get_current_doc_score(
                    &doc_lengths[current as usize - 1],
                    average_doc_length,
                    &params,
                    doc_lengths.len() as u32,
                );
            }
        }

        let does_length_exceed = pq.len() >= max_size;
        if does_length_exceed {
            let does_score_exceed = score > pq.peek().unwrap().0.0.score;
            if does_score_exceed {
                pq.push(Reverse(FloatDoc(DocData {
                    docid: current as u32,
                    score,
                })));
                pq.pop();
                threshold = pq.peek().unwrap().0.0.score;
                while pivot < n && ub[pivot] <= threshold {
                    pivot += 1;
                }
            }
        } else {
            pq.push(Reverse(FloatDoc(DocData {
                docid: current as u32,
                score,
            })));
        }
        current = next;
    }
    let mut doc_ids = Vec::with_capacity(max_size);
    while !pq.is_empty() {
        if let Some(doc) = pq.pop() {
            doc_ids.push((doc.0.0.docid, doc.0.0.score));
        }
    }
    doc_ids
}

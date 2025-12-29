use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::u32;

use crate::query_processor::retrieval_algorithms::utils::{
    DocData, FloatDoc, sort_by_doc_id, swap_down,
};
use crate::query_processor::term_iterator::TermIterator;
use crate::scoring::bm_25::BM25Params;

pub fn block_max_wand(
    mut term_iterators: Vec<TermIterator>,
    doc_lengths: &Vec<u32>,
    average_doc_length: f32,
) -> Vec<(u32, f32)> {
    let max_size = 20;
    let mut pq: BinaryHeap<Reverse<FloatDoc>> = BinaryHeap::with_capacity(max_size);
    let mut threshold = 0.0;
    sort_by_doc_id(&mut term_iterators);
    let params = BM25Params::default();

    loop {
        let mut score: f32 = 0.0;
        let mut pivot = 0;
        while pivot < term_iterators.len() {
            let is_complete = term_iterators[pivot].is_complete();
            if is_complete {
                break;
            }
            score += term_iterators[pivot].get_max_score();
            if score > threshold {
                break;
            }
            pivot += 1;
        }
        if score <= threshold {
            break;
        }
        let pivot_id = term_iterators[pivot].get_current_doc_id();
        while pivot + 1 < term_iterators.len()
            && term_iterators[pivot + 1].get_current_doc_id() == pivot_id
        {
            pivot += 1;
        }
        let mut pivot_score = 0.0;
        let mut next = u64::MAX;
        for i in 0..pivot + 1 {
            // Shallow move
            term_iterators[i]
                .block_max_iterator
                .advance(pivot_id as u32);
            pivot_score += term_iterators[i].get_block_max_score();
            if (term_iterators[i].get_block_max_last_doc_id()) < next {
                next = term_iterators[i].get_block_max_last_doc_id();
            }
        }
        if pivot_score >= threshold {
            if pivot_id == term_iterators[0].get_current_doc_id() {
                let mut score = 0.0;
                for i in 0..pivot + 1 {
                    score += term_iterators[i].get_current_doc_score(
                        &doc_lengths[pivot_id as usize - 1],
                        average_doc_length,
                        &params,
                        doc_lengths.len() as u32,
                    );
                    pivot_score = pivot_score - term_iterators[i].get_block_max_score()
                        + term_iterators[i].get_current_doc_score(
                            &doc_lengths[pivot_id as usize - 1],
                            average_doc_length,
                            &params,
                            doc_lengths.len() as u32,
                        );
                    if pivot_score <= threshold {
                        break;
                    }
                }

                for i in 0..pivot + 1 {
                    term_iterators[i].next();
                }
                pq.push(Reverse(FloatDoc(DocData {
                    docid: pivot_id as u32,
                    score,
                })));
                if pq.len() > max_size {
                    pq.pop();
                }
                threshold = pq.peek().unwrap().0.0.score;
                sort_by_doc_id(&mut term_iterators);
            } else {
                while term_iterators[pivot].get_current_doc_id() == pivot_id {
                    pivot -= 1;
                }
                term_iterators[pivot].advance(pivot_id as u32);
                swap_down(&mut term_iterators, pivot);
            }
        } else {
            while pivot < term_iterators.len() - 1
                && next > term_iterators[pivot + 1].get_current_doc_id()
            {
                next = term_iterators[pivot + 1].get_current_doc_id();
            }

            if next <= pivot_id {
                next += 1;
            }
            term_iterators[pivot].advance(next as u32);
            swap_down(&mut term_iterators, pivot);
        }
    }
    let mut doc_ids = Vec::with_capacity(max_size);
    while !pq.is_empty() {
        if let Some(doc) = pq.pop() {
            doc_ids.push((doc.0.0.docid, doc.0.0.score));
        }
    }
    doc_ids
}

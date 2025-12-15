use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::u32;

use crate::query_processor::retrieval_algorithms::utils::{
    DocData, FloatDoc, sort_by_doc_id, swap_down,
};
use crate::query_processor::term_iterator::TermIterator;

pub fn block_max_wand(mut term_iterators: Vec<TermIterator>) -> Vec<u32> {
    let mut pq: BinaryHeap<Reverse<FloatDoc>> = BinaryHeap::with_capacity(20);
    let mut threshold = 0.0;
    sort_by_doc_id(&mut term_iterators);

    loop {
        let mut score: f32 = 0.0;
        let mut pivot = 0;
        while pivot < term_iterators.len() {
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
        while pivot < term_iterators.len() - 1
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
            if (term_iterators[i].get_block_max_last_doc_id() as u64) < next {
                next = term_iterators[i].get_block_max_last_doc_id() as u64;
            }
        }
        if pivot_score >= threshold {
            if pivot_id == term_iterators[0].get_current_doc_id() {
                let mut score = 0.0;
                for i in 0..pivot + 1 {
                    score += term_iterators[i].get_current_doc_score();
                    pivot_score = pivot_score - term_iterators[i].get_block_max_score()
                        + term_iterators[i].get_current_doc_score();
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
                && next > term_iterators[pivot].get_current_doc_id()
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
    let mut doc_ids = Vec::new();
    while !pq.is_empty() {
        if let Some(doc) = pq.pop() {
            doc_ids.push(doc.0.0.docid);
        }
    }
    doc_ids
}

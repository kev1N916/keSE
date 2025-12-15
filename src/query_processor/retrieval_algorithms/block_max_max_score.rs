use std::{cmp::Reverse, collections::BinaryHeap, u32};

use crate::query_processor::{
    retrieval_algorithms::utils::{DocData, FloatDoc},
    term_iterator::TermIterator,
};

pub fn block_max_max_score(mut term_iterators: Vec<TermIterator>) -> Vec<u32> {
    term_iterators.sort_by(|a, b| a.get_max_score().total_cmp(&b.get_max_score()));
    let n = term_iterators.len();
    let mut ub = vec![0.0; term_iterators.len()];
    ub[0] = term_iterators[0].get_max_score();
    for i in 1..n {
        ub[i] += ub[i - 1] + term_iterators[i].get_max_score();
    }
    let mut pivot = 0;
    let mut threshold = 0.0;
    let mut pq: BinaryHeap<Reverse<FloatDoc>> = BinaryHeap::with_capacity(20);
    let mut current = u64::MAX;
    for term_iterator in &term_iterators {
        current = current.min(term_iterator.get_current_doc_id());
    }

    while pivot < n && current != 0 {
        let mut score = 0.0;
        let mut next = u64::MAX;

        for i in pivot..n {
            if term_iterators[i].get_current_doc_id() == current {
                score += term_iterators[i].get_current_doc_score();
                term_iterators[i].next();
            }
            if term_iterators[i].get_current_doc_id() < next {
                next = term_iterators[i].get_current_doc_id();
            }
        }

        if score + ub[pivot - 1] > threshold {
            let mut bub = vec![0.0; term_iterators.len()];
            term_iterators[0].move_block_max_iterator(current as u32);
            bub[0] = term_iterators[0].get_block_max_score();
            for i in 1..pivot {
                term_iterators[i].move_block_max_iterator(current as u32);
                bub[i] = bub[i - 1] + term_iterators[i].get_block_max_score();
            }
            for i in (0..pivot).rev() {
                if score + bub[i] <= threshold {
                    break;
                }
                term_iterators[i].advance(current as u32);
                if term_iterators[i].get_current_doc_id() == current {
                    score += term_iterators[i].get_current_doc_score()
                }
            }

            let will_pop = pq.len() >= 20 && score > pq.peek().unwrap().0.0.score;
            if will_pop {
                pq.push(Reverse(FloatDoc(DocData {
                    docid: current as u32,
                    score,
                })));
                threshold = pq.peek().unwrap().0.0.score;
                while pivot < n && ub[pivot] <= threshold {
                    pivot += 1;
                }
            }
        }
        current = next;
    }

    let mut doc_ids = Vec::new();
    while !pq.is_empty() {
        if let Some(doc) = pq.pop() {
            doc_ids.push(doc.0.0.docid);
        }
    }
    doc_ids
}

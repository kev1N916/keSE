use std::collections::BinaryHeap;
use std::{cmp::Reverse, f32};

use priority_queue::PriorityQueue;

use crate::{
    query_processor::{
        retrieval_algorithms::utils::{DocData, FloatDoc, sort_by_doc_id, swap_down},
        term_iterator::TermIterator,
    },
    scoring::bm_25::BM25Params,
};

pub fn wand(
    mut term_iterators: Vec<TermIterator>,
    doc_lengths: &Vec<u32>,
    average_doc_length: f32,
) -> Vec<(u32, f32)> {
    let max_docs = 50;
    let mut pq: BinaryHeap<Reverse<FloatDoc>> = BinaryHeap::with_capacity(max_docs as usize);
    let mut threshold = 0.0;
    sort_by_doc_id(&mut term_iterators);
    let params = BM25Params::default();

    loop {
        // println!("threshold{}", threshold);
        let mut score: f32 = 0.0;
        let mut pivot = 0;
        while pivot < term_iterators.len() {
            let is_complete = term_iterators[pivot].is_complete();
            println!(
                "{} {:?}",
                is_complete,
                term_iterators[pivot].get_current_doc_id()
            );
            if is_complete {
                break;
            }
            // println!("{}", term_iterators[pivot].get_max_score());
            score += term_iterators[pivot].get_max_score();
            if score > threshold {
                println!("{} {}", score, threshold);
                break;
            }
            pivot += 1;
        }
        if score <= threshold {
            break;
        }
        let pivot_id = term_iterators[pivot].get_current_doc_id();
        if pivot_id == term_iterators[0].get_current_doc_id() {
            let mut pivot_score = 0.0;

            for i in 0..term_iterators.len() {
                if term_iterators[i].get_current_doc_id() != pivot_id {
                    break;
                }
                pivot_score += term_iterators[i].get_current_doc_score(
                    &doc_lengths[pivot_id as usize - 1],
                    average_doc_length,
                    &params,
                    doc_lengths.len() as u32,
                );
                term_iterators[i].next();
            }
            pq.push(Reverse(FloatDoc(DocData {
                docid: pivot_id as u32,
                score: pivot_score,
            })));
            if pq.len() > max_docs {
                pq.pop();
            }
            threshold = pq.peek().unwrap().0.0.score;
            sort_by_doc_id(&mut term_iterators);
        } else {
            println!("but why here");
            // println!("{}", pivot);
            println!("{}", pq.len());
            while pivot > 0 && term_iterators[pivot].get_current_doc_id() == pivot_id {
                pivot -= 1;
            }

            println!(
                "{} {} ",
                pivot_id,
                term_iterators[pivot].get_current_doc_id()
            );

            term_iterators[pivot].advance(pivot_id as u32);
            swap_down(&mut term_iterators, pivot);
        }
    }

    println!("size of queue {}", pq.len());
    let mut doc_ids = Vec::with_capacity(pq.len());
    while !pq.is_empty() {
        if let Some(doc) = pq.pop() {
            doc_ids.push((doc.0.0.docid, doc.0.0.score));
        }
    }
    doc_ids
}

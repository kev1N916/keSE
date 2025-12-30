use std::cmp::Ordering;

use crate::query_processor::term_iterator::TermIterator;

#[derive(Debug, PartialEq)]
pub struct DocData {
    pub docid: u32,
    pub score: f32,
}

#[derive(Debug, PartialEq)]
pub struct FloatDoc(pub DocData);

impl Eq for FloatDoc {}

impl Ord for FloatDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .score
            .total_cmp(&other.0.score)
            .then_with(|| self.0.docid.cmp(&other.0.docid))
    }
}

impl PartialOrd for FloatDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn sort_by_doc_id(term_iterators: &mut Vec<TermIterator>) {
    term_iterators.sort_by(|a, b| a.get_current_doc_id().cmp(&b.get_current_doc_id()));
}
pub fn swap_down(term_iterators: &mut Vec<TermIterator>, pivot: usize) {
    let mut temp = pivot;
    while temp + 1 < term_iterators.len()
        && term_iterators[temp].get_current_doc_id() > term_iterators[temp + 1].get_current_doc_id()
    {
        term_iterators.swap(temp, temp + 1);
        temp += 1;
    }
}

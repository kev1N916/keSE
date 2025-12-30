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

pub fn merge_all_postings(lists: Vec<Vec<Posting>>) -> Vec<Posting> {
    let total_size: usize = lists.iter().map(|l| l.len()).sum();
    let mut result = Vec::with_capacity(total_size);
    let mut heap = BinaryHeap::new();

    let mut iterators: Vec<_> = lists.into_iter().map(|list| list.into_iter()).collect();

    for (idx, iter) in iterators.iter_mut().enumerate() {
        if let Some(posting) = iter.next() {
            heap.push(PostingWithSource {
                posting,
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
        if let Some(next_posting) = iterators[list_idx].next() {
            heap.push(PostingWithSource {
                posting: next_posting,
                list_idx,
                pos_in_list: pos_in_list + 1,
            });
        }
    }
    result
}

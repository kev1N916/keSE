use crate::query_processor::term_iterator::TermIterator;

// these are both conjunctive algorithms

pub fn binary_merge(mut term_iterators: Vec<TermIterator>) -> Vec<u32> {
    term_iterators.sort_by(|a, b| a.get_no_of_postings().cmp(&b.get_no_of_postings()));
    let mut doc_ids;
    doc_ids = term_iterators[0].get_all_doc_ids();
    if term_iterators.len() == 1 {
        return doc_ids;
    }
    for term_iterator in &mut term_iterators[1..] {
        let mut temp = Vec::new();
        for doc_id in &doc_ids {
            term_iterator.advance(*doc_id);
            if term_iterator.get_current_doc_id() == *doc_id as u64 {
                temp.push(*doc_id);
            }
        }
        doc_ids = temp;
    }
    doc_ids
}

// supposedly faster
pub fn holistic_binary_merge(mut term_iterators: Vec<TermIterator>) -> Vec<u32> {
    term_iterators.sort_by(|a, b| a.get_no_of_postings().cmp(&b.get_no_of_postings()));
    let mut current = term_iterators[0].get_current_doc_id();
    let mut i = 1;
    let mut doc_ids = Vec::new();

    while term_iterators[0].has_next() {
        while i < term_iterators.len() {
            term_iterators[i].advance(current as u32);
            if term_iterators[i].get_current_doc_id() > current {
                let doc_id_to_advance = term_iterators[i].get_current_doc_id();
                term_iterators[0].advance(doc_id_to_advance as u32);
                if term_iterators[0].get_current_doc_id() > doc_id_to_advance {
                    current = term_iterators[0].get_current_doc_id();
                } else {
                    current = doc_id_to_advance;
                    i = 0;
                }
                break;
            }
            i += 1;
        }

        if i == term_iterators.len() {
            doc_ids.push(current as u32);
            term_iterators[0].next();
            current = term_iterators[0].get_current_doc_id();
            i = 1;
        }
    }
    doc_ids
}

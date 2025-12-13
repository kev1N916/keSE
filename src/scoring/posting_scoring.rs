use std::cmp::Ordering;

use crate::utils::posting::Posting;

#[derive(Debug, Clone, Copy)]
pub struct ScoredDoc {
    pub doc_id: u32,
    pub score: f32,
}

impl PartialEq for ScoredDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredDoc {}

impl PartialOrd for ScoredDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        // For max heap by score (highest first)
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
    }
}

// can get during processing
pub fn get_document_frequency(posting: &Vec<Posting>) -> f32 {
    posting.len() as f32
}
// can get during processing
pub fn get_term_frequency(posting: &Posting) -> f32 {
    posting.positions.len() as f32
}
// can get during processing
pub fn get_inverse_document_frequency(document_frequency: f32, total_documents: u32) -> f32 {
    f32::log10((total_documents as f32) / (document_frequency as f32))
}
// can get during processing
pub fn get_tf_idf_weight(term_frequency: f32, inverse_document_frequency: f32) -> f32 {
    term_frequency * inverse_document_frequency
}

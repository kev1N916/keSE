// --- Custom FloatDoc to Handle f32 Ordering ---

use std::cmp::Ordering;

// 1. Define the document structure
#[derive(Debug, PartialEq)]
pub struct DocData {
    pub docid: u32,
    pub score: f32, // The raw floating point score
}

// 2. Wrapper Struct: Used only for the BinaryHeap to derive ordering
// We must implement Eq, Ord, PartialOrd, and PartialEq manually because of the f32.
// We use the derived PartialEq but manually implement the rest based on score.
#[derive(Debug, PartialEq)]
pub struct FloatDoc(pub DocData);

impl Eq for FloatDoc {}

// Implement Ord: This defines the total ordering for the heap
impl Ord for FloatDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare the f32 scores.
        // We use the total_cmp method, which returns an Ordering and handles NaN.
        // NaN is treated as the smallest possible value by total_cmp.
        self.0
            .score
            .total_cmp(&other.0.score)
            .then_with(|| self.0.docid.cmp(&other.0.docid)) // Use docid as tie-breaker
    }
}

impl PartialOrd for FloatDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Since we implemented Ord using total_cmp (which guarantees total order),
        // we can safely just call the full comparison logic here.
        Some(self.cmp(other))
    }
}

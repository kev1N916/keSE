pub mod binary_merge;
pub mod block_max_max_score;
pub mod block_max_wand;
pub mod max_score;
mod utils;
pub mod wand;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryAlgorithm {
    Wand,
    BlockMaxWand,
    BlockMaxMaxScore,
    MaxScore,
    Boolean,
}

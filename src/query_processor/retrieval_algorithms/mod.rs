pub mod binary_merge;
pub mod block_max_max_score;
pub mod block_max_wand;
pub mod max_score;
mod utils;
pub mod wand;

pub enum RankingAlgorithm {
    Wand,
    BlockMaxWand,
    BlockMaxMaxScore,
    MaxScore,
    Boolean,
}

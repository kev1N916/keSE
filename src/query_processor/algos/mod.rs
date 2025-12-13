pub mod binary_merge;
pub mod block_max_max_score;
pub mod block_max_wand;
pub mod max_score;
pub mod utils;
pub mod wand;

pub enum RankingAlgorithm {
    Wand,
    Block_Max_Wand,
    Block_Max_Max_Score,
    Max_Score,
}

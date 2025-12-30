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

impl QueryAlgorithm {
    pub fn to_string(&self) -> String {
        match self {
            QueryAlgorithm::BlockMaxMaxScore => String::from("Block Max Max Score (BMMS)"),
            QueryAlgorithm::BlockMaxWand => String::from("Block Max Wand (BMW)"),
            QueryAlgorithm::Wand => String::from("WAND"),
            QueryAlgorithm::Boolean => String::from("Boolean"),
            QueryAlgorithm::MaxScore => String::from("Max Score (MS)"),
        }
    }
}

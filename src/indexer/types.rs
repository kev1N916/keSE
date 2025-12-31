use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct WikiArticle {
    pub url: String,
    pub text: Vec<Vec<String>>,
    pub id: String,
    pub title: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WikiArticle1 {
    pub url: String,
    pub text: String,
    pub title: String,
}

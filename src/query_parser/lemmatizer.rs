use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Lemmatizer {
    lemmas: HashMap<String, String>,
}

impl Lemmatizer {
    pub fn lemmatize(&self, word: &str) -> Option<&String> {
        self.lemmas.get(word)
    }
}

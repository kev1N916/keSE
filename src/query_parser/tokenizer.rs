use std::{env, io};
// A custom error type to represent our possible errors
#[derive(Debug)]
pub enum TokenizationError {
    InitializationError,
    InvalidUtf8(std::string::FromUtf8Error),
    EmptyInput,
    LemmatizerError(std::io::Error),
}

impl From<std::string::FromUtf8Error> for TokenizationError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        TokenizationError::InvalidUtf8(error)
    }
}

impl From<std::io::Error> for TokenizationError {
    fn from(error: std::io::Error) -> Self {
        TokenizationError::LemmatizerError(error)
    }
}
pub struct Token {
    pub position: u32,
    pub word: String,
}

#[derive(Debug, Clone)]
pub struct Lemmatizer {
    lemmas: HashMap<String, String>,
}

impl Lemmatizer {
    pub fn lemmatize(&self, word: &str) -> Option<String> {
        let is_word_present = self.lemmas.contains_key(word);
        if !is_word_present {
            return None;
        } else {
            Some(self.lemmas.get(word).unwrap().clone())
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchTokenizer {
    lemmatizer: Lemmatizer,
}

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn parse_lemma(file_path: &str) -> Result<HashMap<String, String>, io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut word_map: HashMap<String, String> = HashMap::new();

    for line in reader.lines() {
        let line = line?;

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        // Find the first comma to split key and values
        if let Some(comma_pos) = line.find(',') {
            let key = &line[..comma_pos].trim().to_string();
            let values_str = &line[comma_pos + 1..];

            // Remove surrounding quotes if present
            let values_str = values_str.trim().trim_matches('"');

            let all_words: Vec<String> = values_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            for word in all_words {
                word_map.insert(word, key.clone());
            }
        }
    }

    Ok(word_map)
}

pub fn clean_word(word: &str) -> String {
    return word
        .to_lowercase()
        .chars()
        .skip_while(|c| !c.is_alphanumeric())
        .collect::<String>()
        .chars()
        .rev()
        .skip_while(|c| !c.is_alphanumeric())
        .collect::<String>()
        .chars()
        .rev()
        .collect();
}

pub struct TokenizeQueryResult {
    pub unigram: Vec<Token>,
    pub bigram: Vec<Token>,
}
impl SearchTokenizer {
    pub fn new() -> Result<SearchTokenizer, io::Error> {
        let current_dir = env::current_dir()?;
        let path_as_string = format!("{}", current_dir.display());
        let mut path = path_as_string.to_string();
        path += "/src";
        let lemmatizer_path = path.clone() + "/lemmas.txt";

        let lemmas = parse_lemma(&lemmatizer_path)?;
        let lemmatizer = Lemmatizer { lemmas: lemmas };
        Ok(SearchTokenizer {
            lemmatizer: lemmatizer,
        })
    }

    pub fn tokenize_query(
        &self,
        sentences: &String,
    ) -> Result<TokenizeQueryResult, TokenizationError> {
        if sentences.trim().is_empty() {
            return Err(TokenizationError::EmptyInput);
        }

        let mut unigram_tokens: Vec<Token> = Vec::new();
        let mut bigram_tokens: Vec<Token> = Vec::new();
        let mut position = 0;
        let mut prev_lemma: Option<String> = None;

        for word in sentences.split_whitespace() {
            let cleaned_word = clean_word(word);

            if !cleaned_word.is_empty() {
                let lemma = self.lemmatizer.lemmatize(&cleaned_word);

                let current_lemma = match lemma {
                    Some(lemma) => {
                        unigram_tokens.push(Token {
                            position,
                            word: lemma.clone(),
                        });
                        lemma
                    }
                    None => {
                        unigram_tokens.push(Token {
                            position,
                            word: cleaned_word.clone(),
                        });
                        cleaned_word.clone()
                    }
                };

                if let Some(prev) = &prev_lemma {
                    bigram_tokens.push(Token {
                        position: position - 1,
                        word: format!("{} {}", prev, current_lemma),
                    });
                }

                prev_lemma = Some(current_lemma);
            }

            position = position + 1;
        }

        Ok(TokenizeQueryResult {
            unigram: unigram_tokens,
            bigram: bigram_tokens,
        })
    }

    pub fn tokenize(&self, sentences: String) -> Vec<Token> {
        if sentences.trim().is_empty() {
            return Vec::new();
        }

        let mut tokens = Vec::new();
        let mut position = 0;
        // let mut prev_lemma: Option<String> = None;

        for word in sentences.split_whitespace() {
            let cleaned_word = clean_word(word);

            if !cleaned_word.is_empty() {
                let lemma = self.lemmatizer.lemmatize(&cleaned_word);

                match lemma {
                    Some(lemma) => {
                        tokens.push(Token {
                            position: position,
                            word: lemma.clone(),
                        });
                        lemma
                    }
                    None => {
                        tokens.push(Token {
                            position: position,
                            word: cleaned_word.clone(),
                        });
                        cleaned_word.clone()
                    }
                };

                // if let Some(prev) = &prev_lemma {
                //     tokens.push(Token {
                //         position: position - 1,
                //         word: format!("{} {}", prev, current_lemma),
                //     });
                // }

                // prev_lemma = Some(current_lemma);
            }

            position = position + 1;
        }

        tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a test tokenizer
    fn create_test_tokenizer() -> SearchTokenizer {
        SearchTokenizer::new().expect("Failed to create tokenizer")
    }

    #[test]
    fn test_new_tokenizer_creation() {
        let result = SearchTokenizer::new();
        assert!(result.is_ok(), "Should successfully create tokenizer");
    }

    // #[test]
    // fn test_multiple_words() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "the quick brown fox".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input).expect("Should tokenize successfully");

    //     assert_eq!(result.len(), 4);

    //     // Check positions are correctly assigned
    //     for (i, token) in result.iter().enumerate() {
    //         assert_eq!(token.position, i);
    //         assert!(!token.word.is_empty());
    //         assert!(!token.part_of_speech.is_empty());
    //     }
    // }

    // #[test]
    // fn test_punctuation_handling() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "Hello, world! How are you?".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input).expect("Should tokenize successfully");

    //     // Should handle punctuation according to clean_word function
    //     // Verify no tokens contain punctuation marks
    //     for token in &result {
    //         assert!(!token.word.contains(&[',', '!', '?'][..]));
    //     }
    // }

    // #[test]
    // fn test_extra_whitespace() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "  word1    word2  \n\t  word3  ".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input).expect("Should tokenize successfully");

    //     assert_eq!(result.len(), 3);
    //     assert_eq!(result[0].position, 0);
    //     assert_eq!(result[1].position, 1);
    //     assert_eq!(result[2].position, 2);
    // }

    // #[test]
    // fn test_empty_words_after_cleaning() {
    //     let tokenizer = create_test_tokenizer();
    //     // Assuming punctuation-only tokens get cleaned to empty strings
    //     let input = "word1 ,,, !!! word2".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input).expect("Should tokenize successfully");

    //     // Should only have tokens for actual words, not punctuation-only tokens
    //     assert!(result.len() >= 2); // At least word1 and word2

    //     // But positions should still increment for all split items
    //     // This tests the position counting logic
    // }

    // #[test]
    // fn test_stemming_functionality() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "running runs ran".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input).expect("Should tokenize successfully");

    //     // All should stem to "run" (assuming English stemmer works correctly)
    //     for token in &result {
    //         assert_eq!(token.word, "run");
    //     }
    // }

    // #[test]
    // fn test_pos_tagging() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "cats run quickly".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input).expect("Should tokenize successfully");

    //     assert_eq!(result.len(), 3);

    //     // Verify each token has a POS tag
    //     for token in &result {
    //         assert!(!token.part_of_speech.is_empty());
    //     }
    // }

    // #[test]
    // fn test_unicode_support() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "café naïve résumé".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input);

    //     // Should handle Unicode characters properly
    //     assert!(result.is_ok());
    //     let tokens = result.unwrap();
    //     assert_eq!(tokens.len(), 3);
    // }

    // #[test]
    // fn test_long_text() {
    //     let tokenizer = create_test_tokenizer();
    //     let long_text = "word ".repeat(1000);
    //     let input = long_text.as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input);

    //     assert!(result.is_ok());
    //     let tokens = result.unwrap();
    //     assert_eq!(tokens.len(), 1000);

    //     // Verify positions are correct
    //     for (i, token) in tokens.iter().enumerate() {
    //         assert_eq!(token.position, i);
    //     }
    // }

    // #[test]
    // fn test_numbers_and_mixed_content() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "item123 test-case version2.0".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input);

    //     assert!(result.is_ok());
    //     // Behavior depends on your clean_word implementation
    //     let tokens = result.unwrap();
    //     assert!(tokens.len() > 0);
    // }

    // // Integration test with realistic search queries
    // #[test]
    // fn test_realistic_search_queries() {
    //     let tokenizer = create_test_tokenizer();

    //     let queries = vec![
    //         "machine learning algorithms",
    //         "best restaurants near me",
    //         "how to cook pasta",
    //         "weather forecast tomorrow",
    //     ];

    //     for query in queries {
    //         let input = query.as_bytes().to_vec();
    //         let result = tokenizer.tokenize(input);
    //         assert!(result.is_ok(), "Failed to tokenize query: {}", query);

    //         let tokens = result.unwrap();
    //         assert!(tokens.len() > 0);

    //         // Verify all tokens have required fields
    //         for token in tokens {
    //             assert!(!token.word.is_empty());
    //             assert!(!token.part_of_speech.is_empty());
    //         }
    //     }
    // }

    // // Property-based test helper
    // #[test]
    // fn test_position_invariant() {
    //     let tokenizer = create_test_tokenizer();
    //     let input = "a b c d e f g".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input).expect("Should tokenize");

    //     // Property: positions should be sequential starting from 0
    //     for (expected_pos, token) in result.iter().enumerate() {
    //         assert_eq!(token.position, expected_pos);
    //     }
    // }

    // // Mock/Error condition tests
    // #[test]
    // fn test_tagger_returns_empty() {
    //     // This would require mocking the tagger to return empty results
    //     // You might need to refactor to inject dependencies for this test
    //     let tokenizer = create_test_tokenizer();
    //     let input = "word".as_bytes().to_vec();
    //     let result = tokenizer.tokenize(input);

    //     // Depending on implementation, might skip tokens with no POS tags
    //     assert!(result.is_ok());
    // }
}

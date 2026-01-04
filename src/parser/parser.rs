// A custom error type to represent our possible errors
#[derive(Debug)]
pub enum TokenizationError {
    EmptyInput,
}

#[derive(Debug)]
pub struct Token {
    pub position: u32,
    pub word: String,
}

const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is", "it",
    "its", "of", "on", "that", "the", "to", "was", "will", "with", "the", "this", "but", "they",
    "have", "had", "what", "when", "where", "who", "which", "why", "how", "all", "each", "every",
    "both", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own",
    "same", "so", "than", "too", "very", "can", "will", "just", "should", "now",
];

#[derive(Debug, Clone)]
pub struct Parser {
    stop_word_set: HashSet<String>,
}

use std::collections::HashSet;
pub fn clean_word(word: &str) -> &str {
    // let trimmed = word.trim_matches(|c: char| !c.is_alphanumeric());
    // unidecode(
    word.trim_matches(|c: char| !c.is_alphanumeric())
    // .to_lowercase()
    // )
}

#[inline]
pub fn is_valid_text(text: &str) -> bool {
    if text.is_empty()
        || text.starts_with('-')
        || text.ends_with('-')
        // || text.len() > 30
        || !text.chars().all(|c| c.is_ascii_alphabetic() || c == '-')
    {
        return false;
    }
    true
}

#[inline]
fn is_valid_date(text: &str) -> bool {
    let b = text.as_bytes();
    match b.len() {
        1 => b[0].is_ascii_digit(),
        2 => b[0].is_ascii_digit() && b[1].is_ascii_digit(),
        3 => b[0].is_ascii_digit() && b[1].is_ascii_digit() && b[2].is_ascii_digit(),
        4 => {
            b[0].is_ascii_digit()
                && b[1].is_ascii_digit()
                && b[2].is_ascii_digit()
                && b[3].is_ascii_digit()
                && b[0] >= b'1'
                && b[0] <= b'2'
        }
        _ => false,
    }
}

// pub struct TokenizeQueryResult {
//     pub unigram: Vec<Token>,
// }

impl Parser {
    pub fn new() -> Parser {
        let stop_word_set: HashSet<String> = STOP_WORDS.iter().map(|&s| s.to_string()).collect();
        Parser { stop_word_set }
    }

    pub fn tokenize(&self, sentences: &str, tokens: &mut Vec<Token>) {
        if sentences.trim().is_empty() {
            return;
        }

        let mut position = 0;

        for word in sentences.split_whitespace() {
            let cleaned_word = clean_word(word);
            if !cleaned_word.is_empty() {
                let new_word = cleaned_word.to_lowercase();
                if !self.stop_word_set.contains(&new_word)
                    && (is_valid_text(&new_word) || is_valid_date(&new_word))
                {
                    tokens.push(Token {
                        position,
                        word: new_word,
                    });
                    position += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_word_and_validate() {
        let word = "left-right";
        let cleaned_word = clean_word(word);
        println!("{}", cleaned_word);
        // let parts: Vec<&str> = word.split('-').filter(|s| !s.is_empty()).collect();
        // let parser = Parser::new();
        // let buffer = Vec::new();
        // let parts: Vec<&str> = word.split('-').filter(|s| !s.is_empty()).collect();
        // // let splt = cleaned_word.split_terminator("-").collect();
        // println!("{:?}", parts);
        println!("{}", is_valid_text(&cleaned_word));
    }

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

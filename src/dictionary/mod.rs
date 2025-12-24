use std::collections::BTreeMap;

use crate::utils::posting::Posting;
#[derive(Debug, Clone)]
pub struct Dictionary {
    pub no_of_terms: u32,
    current_size: u32,
    pub dictionary: BTreeMap<String, Vec<Posting>>,
}

impl Dictionary {
    pub fn new() -> Dictionary {
        return Dictionary {
            no_of_terms: 0,
            current_size: 0,
            dictionary: BTreeMap::new(),
        };
    }

    // max size of dictionary in bytes
    // we are keeping it as 200 MB
    pub fn max_size(&self) -> u32 {
        return 200_000_000;
    }
    pub fn size(&self) -> u32 {
        self.current_size
    }
    pub fn clear(&mut self) {
        self.dictionary.clear();
        self.current_size = 0;
        self.no_of_terms = 0;
    }

    //   pub fn capacity(& self) {
    //     self.dictionary.
    // }

    pub fn does_term_already_exist(&mut self, term: &str) -> bool {
        return self.dictionary.contains_key(term);
    }

    pub fn add_term_posting(&mut self, term: &str, posting: Vec<Posting>) {
        for posting in &posting {
            let posting_length = posting.positions.len() as u32;
            self.current_size += 4 + 4 * posting_length;
        }
        self.dictionary.insert(String::from(term), posting);
        self.current_size += 4;
        self.current_size += term.len() as u32;
    }

    pub fn get_postings(&self, term: &str) -> Option<&Vec<Posting>> {
        self.dictionary.get(term)
    }

    pub fn add_term(&mut self, term: &str) {
        if !self.does_term_already_exist(term) {
            self.dictionary.insert(term.to_string(), Vec::new());
            self.current_size += 4;
            self.current_size += term.len() as u32;
            self.no_of_terms += 1;
        }
    }

    pub fn append_to_term(&mut self, term: &str, posting: Posting) {
        if let Some(postings_list) = self.dictionary.get_mut(term) {
            let posting_length = posting.positions.len() as u32;
            postings_list.push(posting);
            self.current_size += 4 + 4 * posting_length + 4;
            // postings_list.shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod dictionary_tests {
    use super::*;

    #[test]
    fn test_does_term_already_exist_empty_dictionary() {
        let mut dict = Dictionary::new();
        assert!(!dict.does_term_already_exist("test"));
    }

    #[test]
    fn test_add_term() {
        let mut dict = Dictionary::new();
        dict.add_term("hello");

        assert!(dict.does_term_already_exist("hello"));
        assert_eq!(dict.dictionary.len(), 1);

        // Verify the term has an empty posting list
        let postings = dict.get_postings("hello");
        assert!(postings.is_some());
        assert_eq!(postings.unwrap().len(), 0);
    }

    #[test]
    fn test_add_term_posting() {
        let mut dict = Dictionary::new();
        let postings = vec![
            Posting::new(1, vec![5, 10, 15]),
            Posting::new(2, vec![3, 8]),
        ];

        dict.add_term_posting("rust", postings.clone());

        assert!(dict.does_term_already_exist("rust"));
        let retrieved_postings = dict.get_postings("rust");
        assert!(retrieved_postings.is_some());
        assert_eq!(*retrieved_postings.unwrap(), postings);
    }

    #[test]
    fn test_get_postings_nonexistent_term() {
        let mut dict = Dictionary::new();
        let postings = dict.get_postings("nonexistent");
        assert!(postings.is_none());
    }

    #[test]
    fn test_append_to_term() {
        let mut dict = Dictionary::new();
        dict.add_term("programming");

        let posting1 = Posting::new(10, vec![2, 7, 12]);
        let posting2 = Posting::new(20, vec![4, 9]);

        dict.append_to_term("programming", posting1.clone());
        dict.append_to_term("programming", posting2.clone());

        let postings = dict.get_postings("programming").unwrap();
        assert_eq!(postings.len(), 2);
        assert_eq!(postings[0], posting1);
        assert_eq!(postings[1], posting2);
    }

    #[test]
    fn test_append_to_nonexistent_term() {
        let mut dict = Dictionary::new();
        let posting = Posting::new(1, vec![5]);

        // This should not panic, but also shouldn't add the posting
        dict.append_to_term("nonexistent", posting);

        // The term should still not exist
        assert!(!dict.does_term_already_exist("nonexistent"));
    }

    #[test]
    fn test_overwrite_term_posting() {
        let mut dict = Dictionary::new();
        let initial_postings = vec![Posting::new(1, vec![1, 3])];
        let new_postings = vec![Posting::new(2, vec![2, 5, 8]), Posting::new(3, vec![3, 7])];

        dict.add_term_posting("test", initial_postings);
        dict.add_term_posting("test", new_postings.clone());

        let retrieved = dict.get_postings("test").unwrap();
        assert_eq!(*retrieved, new_postings);
        assert_ne!(retrieved.len(), 1); // Should not be the initial posting
    }

    #[test]
    fn test_mixed_operations() {
        let mut dict = Dictionary::new();

        // Add some terms with postings
        dict.add_term_posting("search", vec![Posting::new(1, vec![5])]);
        dict.add_term_posting("engine", vec![Posting::new(2, vec![3])]);

        // Add empty term and then append to it
        dict.add_term("index");
        dict.append_to_term("index", Posting::new(3, vec![7]));

        // Verify all operations worked
        assert!(dict.does_term_already_exist("search"));
        assert!(dict.does_term_already_exist("engine"));
        assert!(dict.does_term_already_exist("index"));

        let search_postings = dict.get_postings("search").unwrap();
        assert_eq!(search_postings.len(), 1);
        assert_eq!(search_postings[0], Posting::new(1, vec![5]));

        let index_postings = dict.get_postings("index").unwrap();
        assert_eq!(index_postings.len(), 1);
        assert_eq!(index_postings[0], Posting::new(3, vec![7]));
    }

    #[test]
    fn test_thread_safety_simulation() {
        // This test simulates what might happen with concurrent access
        // Though we can't easily test true concurrency in unit tests
        let mut dict = Dictionary::new();
        dict.add_term_posting("concurrent", vec![Posting::new(1, vec![1])]);

        // Get multiple references to the same postings
        let postings1 = dict.get_postings("concurrent");
        let postings2 = dict.get_postings("concurrent");
        let p1 = postings1.is_some();
        assert!(p1);
        assert!(postings2.is_some());
        assert_eq!(postings1.unwrap(), postings2.unwrap());

        // Append to the term
        dict.append_to_term("concurrent", Posting::new(2, vec![2]));

        // Get postings again to verify the append worked
        let updated_postings = dict.get_postings("concurrent").unwrap();
        assert_eq!(updated_postings.len(), 2);
    }

    #[test]
    fn test_posting_with_multiple_positions() {
        let mut dict = Dictionary::new();

        // Test posting with multiple positions (typical for search index)
        let posting = Posting::new(42, vec![1, 5, 10, 15, 20]);
        dict.add_term("algorithm");
        dict.append_to_term("algorithm", posting.clone());

        let retrieved_postings = dict.get_postings("algorithm").unwrap();
        assert_eq!(retrieved_postings.len(), 1);
        assert_eq!(retrieved_postings[0].doc_id, 42);
        assert_eq!(retrieved_postings[0].positions, vec![1, 5, 10, 15, 20]);
    }

    #[test]
    fn test_posting_with_empty_positions() {
        let mut dict = Dictionary::new();

        // Test posting with no positions
        let posting = Posting::new(100, vec![]);
        dict.add_term("empty");
        dict.append_to_term("empty", posting.clone());

        let retrieved_postings = dict.get_postings("empty").unwrap();
        assert_eq!(retrieved_postings.len(), 1);
        assert_eq!(retrieved_postings[0].doc_id, 100);
        assert_eq!(retrieved_postings[0].positions.len(), 0);
    }
}

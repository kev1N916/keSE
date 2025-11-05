pub struct StringInMemoryDictPointer {
    pub term_offset: Option<u32>,
    pub term_frequency: u32,
    pub posting_offset: u32,
}
pub struct StringInMemoryDict {
    block_size: u8,
    term_string: String,
    term_array: Vec<StringInMemoryDictPointer>,
}

impl StringInMemoryDict {
    pub fn new(block_size: u8) -> StringInMemoryDict {
        return StringInMemoryDict {
            block_size: block_size,
            term_array: Vec::new(),
            term_string: String::new(),
        };
    }
    pub fn add_term(&mut self, term: &str, posting_offset: u32, term_frequency: u32) {
        self.term_string.push(term.len() as u8 as char);
        self.term_string.push_str(term);
        let is_new_block_beginning: bool = ((self.term_array.len()) as u8) % self.block_size == 0;
        if is_new_block_beginning {
            self.term_array.push(StringInMemoryDictPointer {
                term_offset: Some((self.term_string.len() - term.len() - 1) as u32),
                term_frequency,
                posting_offset,
            });
        } else {
            self.term_array.push(StringInMemoryDictPointer {
                term_offset: None,
                term_frequency,
                posting_offset,
            });
        }
    }

    fn find_term_offset_within_block(&mut self, term: &str, start_term_offset: usize) -> u8 {
        let mut posting_offset: u8 = self.block_size;
        let mut term_offset = start_term_offset;
        for i in 0..self.block_size {
            let term_offset_usize = term_offset as usize;

            if term_offset_usize >= self.term_string.len() {
                return self.block_size;
            }

            let term_length = self.term_string.as_bytes()[term_offset_usize] as u8;

            let string_start = term_offset_usize + 1;
            let string_end = string_start + (term_length as usize);

            if string_end > self.term_string.len() {
                return self.block_size;
            }

            let term_data = &self.term_string[string_start..string_end];
            if term == term_data {
                posting_offset = i;
                break;
            }
            term_offset = term_offset + 1 + term_length as usize;
        }
        posting_offset
    }
    fn get_starting_term_from_block(&mut self, term_offset: usize) -> String {
        let term_offset_usize = term_offset as usize;

        if term_offset_usize >= self.term_string.len() {
            return String::new();
        }

        let term_length = self.term_string.as_bytes()[term_offset_usize] as u8;

        let string_start = term_offset_usize + 1;
        let string_end = string_start + (term_length as usize);

        if string_end > self.term_string.len() {
            return String::new();
        }

        let term_data = &self.term_string[string_start..string_end];
        term_data.to_string()
    }

    // Helper function to find the start of the block containing or before the given index
    fn find_block_start(&self, index: usize) -> i32 {
        if index >= self.term_array.len() {
            return -1;
        }

        // Search backwards from index to find a term with term_offset
        for i in (0..=index).rev() {
            if self.term_array[i].term_offset.is_some() {
                return i as i32;
            }
        }
        -1
    }

    // Helper function to find the start of the next block after the given block start
    fn get_next_block_start(&self, current_block_start: usize) -> i32 {
        let start_search = current_block_start + self.block_size as usize;
        for i in start_search..self.term_array.len() {
            if self.term_array[i].term_offset.is_some() {
                return i as i32;
            }
        }
        -1 // No next block found
    }

    pub fn find(&mut self, term: &str) -> i32 {
        if self.term_array.is_empty() {
            return -1;
        }

        let mut l = 0i32;
        let mut r = (self.term_array.len() - 1) as i32;

        // Find the correct block using binary search
        while l <= r {
            let mid = l + (r - l) / 2;

            // Find the nearest block start at or before mid
            let block_start = self.find_block_start(mid as usize);
            if block_start == -1 {
                return -1; // No valid block found
            }

            // Extract the term_offset first to avoid borrowing conflicts
            let term_offset =
                if let Some(offset) = self.term_array[block_start as usize].term_offset {
                    offset
                } else {
                    return -1;
                };

            let posting_offset = self.term_array[block_start as usize].posting_offset;
            let term_found = self.get_starting_term_from_block(term_offset as usize);
            if term_found.is_empty() {
                return -1;
            }
            match term_found.cmp(&term.to_string()) {
                std::cmp::Ordering::Equal => {
                    // Found exact match at block start
                    return posting_offset as i32;
                }
                std::cmp::Ordering::Greater => {
                    // Block start term is greater than target, search left
                    r = block_start - 1;
                }
                std::cmp::Ordering::Less => {
                    // Block start term is less than target, could be in this block or to the right
                    // First check if it's in the current block
                    let block_offset =
                        self.find_term_offset_within_block(term, term_offset as usize);
                    if block_offset < self.block_size {
                        // Found in current block
                        let block_pointer_index = block_offset as usize + block_start as usize;
                        if block_pointer_index < self.term_array.len() {
                            return self.term_array[block_pointer_index].posting_offset as i32;
                        }
                    }
                    // Not in current block, search right
                    l = self.get_next_block_start(block_start as usize);
                    if l == -1 {
                        break; // No more blocks to search
                    }
                }
            }
        }
        -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_dict_creation() {
        let dict = StringInMemoryDict::new(4);
        assert_eq!(dict.block_size, 4);
        assert!(dict.term_array.is_empty());
        assert!(dict.term_string.is_empty());
    }

    #[test]
    fn test_add_single_term() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("hello", 100, 5);

        assert_eq!(dict.term_array.len(), 1);
        assert_eq!(dict.term_array[0].term_offset, Some(0));
        assert_eq!(dict.term_array[0].term_frequency, 5);
        assert_eq!(dict.term_array[0].posting_offset, 100);

        // Check term_string format: length byte + term
        assert_eq!(dict.term_string.len(), 6); // 1 byte for length + 5 bytes for "hello"
        assert_eq!(dict.term_string.as_bytes()[0], 5); // length of "hello"
        assert_eq!(&dict.term_string[1..], "hello");
    }

    #[test]
    fn test_add_multiple_terms_within_block() {
        let mut dict = StringInMemoryDict::new(3);
        dict.add_term("cat", 10, 2);
        dict.add_term("dog", 20, 3);

        assert_eq!(dict.term_array.len(), 2);

        // First term should have offset (block start)
        assert_eq!(dict.term_array[0].term_offset, Some(0));
        assert_eq!(dict.term_array[0].term_frequency, 2);
        assert_eq!(dict.term_array[0].posting_offset, 10);

        // Second term should not have offset (within same block)
        assert_eq!(dict.term_array[1].term_offset, None);
        assert_eq!(dict.term_array[1].term_frequency, 3);
        assert_eq!(dict.term_array[1].posting_offset, 20);
    }

    #[test]
    fn test_add_terms_across_blocks() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("a", 10, 1);
        dict.add_term("b", 20, 2);
        dict.add_term("c", 30, 3); // This should start a new block

        assert_eq!(dict.term_array.len(), 3);

        // First term (block start)
        assert_eq!(dict.term_array[0].term_offset, Some(0));

        // Second term (within first block)
        assert_eq!(dict.term_array[1].term_offset, None);

        // Third term (new block start)
        assert!(dict.term_array[2].term_offset.is_some());
    }

    #[test]
    fn test_get_starting_term_from_block() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("hello", 10, 1);
        dict.add_term("world", 20, 2);

        let term = dict.get_starting_term_from_block(0);
        assert_eq!(term, "hello");
    }

    #[test]
    fn test_get_starting_term_from_block_invalid_offset() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("test", 10, 1);

        let term = dict.get_starting_term_from_block(1000);
        assert_eq!(term, String::new());
    }

    #[test]
    fn test_find_term_offset_within_block() {
        let mut dict = StringInMemoryDict::new(3);
        dict.add_term("apple", 10, 1);
        dict.add_term("banana", 20, 2);
        dict.add_term("cherry", 30, 3);

        // Find "banana" within the block starting at offset 0
        let offset = dict.find_term_offset_within_block("banana", 0);
        assert_eq!(offset, 1); // Should be at index 1 within the block

        // Find non-existent term
        let offset = dict.find_term_offset_within_block("grape", 0);
        assert_eq!(offset, 3); // Should return block_size
    }

    #[test]
    fn test_find_block_start() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("apple", 100, 5);
        dict.add_term("banana", 200, 3);
        dict.add_term("cherry", 300, 4); // New block
        dict.add_term("date", 400, 2);

        // Test finding block starts
        assert_eq!(dict.find_block_start(0), 0); // First block
        assert_eq!(dict.find_block_start(1), 0); // Still first block
        assert_eq!(dict.find_block_start(2), 2); // Second block
        assert_eq!(dict.find_block_start(3), 2); // Still second block

        // Test out of bounds
        assert_eq!(dict.find_block_start(100), -1);
    }

    #[test]
    fn test_get_next_block_start() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("apple", 100, 5);
        dict.add_term("banana", 200, 3);
        dict.add_term("cherry", 300, 4); // New block
        dict.add_term("date", 400, 2);
        dict.add_term("elderberry", 500, 1); // Another new block
        dict.add_term("fig", 600, 3);

        // Test getting next block starts
        assert_eq!(dict.get_next_block_start(0), 2); // From first block to second
        assert_eq!(dict.get_next_block_start(2), 4); // From second block to third
        assert_eq!(dict.get_next_block_start(4), -1); // No next block after third
    }

    #[test]
    fn test_find_existing_term() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("apple", 100, 5);
        dict.add_term("banana", 200, 3);

        let posting_offset = dict.find("apple");
        assert_eq!(posting_offset, 100);

        let posting_offset = dict.find("banana");
        assert_eq!(posting_offset, 200);
    }

    #[test]
    fn test_find_non_existing_term() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("apple", 100, 5);
        dict.add_term("banana", 200, 3);

        let posting_offset = dict.find("cherry");
        assert_eq!(posting_offset, -1);
    }

    #[test]
    fn test_find_empty_dict() {
        let mut dict = StringInMemoryDict::new(2);
        let posting_offset = dict.find("anything");
        assert_eq!(posting_offset, -1);
    }

    #[test]
    fn test_find_with_multiple_blocks() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("apple", 10, 1);
        dict.add_term("apricot", 20, 2);
        dict.add_term("banana", 30, 3); // New block
        dict.add_term("berry", 40, 4);
        dict.add_term("cherry", 50, 5); // Another new block
        dict.add_term("citrus", 60, 6);

        assert_eq!(dict.find("apple"), 10);
        assert_eq!(dict.find("apricot"), 20);
        assert_eq!(dict.find("banana"), 30);
        assert_eq!(dict.find("berry"), 40);
        assert_eq!(dict.find("cherry"), 50);
        assert_eq!(dict.find("citrus"), 60);
    }

    #[test]
    fn test_block_size_one() {
        let mut dict = StringInMemoryDict::new(1);
        dict.add_term("single", 100, 1);
        dict.add_term("terms", 200, 2);

        // Every term should have an offset since block_size is 1
        assert!(dict.term_array[0].term_offset.is_some());
        assert!(dict.term_array[1].term_offset.is_some());

        assert_eq!(dict.find("single"), 100);
        assert_eq!(dict.find("terms"), 200);
    }

    #[test]
    fn test_large_block_size() {
        let mut dict = StringInMemoryDict::new(255);
        dict.add_term("test1", 10, 1);
        dict.add_term("test2", 20, 2);
        dict.add_term("test3", 30, 3);

        // All should be in the same block
        assert!(dict.term_array[0].term_offset.is_some());
        assert_eq!(dict.term_array[1].term_offset, None);
        assert_eq!(dict.term_array[2].term_offset, None);

        assert_eq!(dict.find("test1"), 10);
        assert_eq!(dict.find("test2"), 20);
        assert_eq!(dict.find("test3"), 30);
    }

    #[test]
    fn test_binary_search_behavior_alphabetical() {
        let mut dict = StringInMemoryDict::new(2);
        // Add terms in alphabetical order to test binary search
        dict.add_term("apple", 10, 1);
        dict.add_term("apricot", 20, 2);
        dict.add_term("banana", 30, 3);
        dict.add_term("berry", 40, 4);
        dict.add_term("cherry", 50, 5);
        dict.add_term("citrus", 60, 6);

        assert_eq!(dict.find("apple"), 10);
        assert_eq!(dict.find("apricot"), 20);
        assert_eq!(dict.find("banana"), 30);
        assert_eq!(dict.find("berry"), 40);
        assert_eq!(dict.find("cherry"), 50);
        assert_eq!(dict.find("citrus"), 60);
        assert_eq!(dict.find("elderberry"), -1);
        assert_eq!(dict.find("aaa"), -1); // Before first term
        assert_eq!(dict.find("zzz"), -1); // After last term
    }

    #[test]
    fn test_unicode_terms() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("café", 100, 1);
        dict.add_term("naïve", 200, 2);

        assert_eq!(dict.find("café"), 100);
        assert_eq!(dict.find("naïve"), 200);
        assert_eq!(dict.find("cafe"), -1); // Different from "café"
    }

    #[test]
    fn test_case_sensitivity() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("Test", 100, 1);
        dict.add_term("test", 200, 2);

        assert_eq!(dict.find("Test"), 100);
        assert_eq!(dict.find("test"), 200);
        assert_eq!(dict.find("TEST"), -1);
    }

    #[test]
    fn test_edge_case_single_character_terms() {
        let mut dict = StringInMemoryDict::new(3);
        dict.add_term("a", 10, 1);
        dict.add_term("b", 20, 2);
        dict.add_term("c", 30, 3);
        dict.add_term("z", 40, 4); // New block

        assert_eq!(dict.find("a"), 10);
        assert_eq!(dict.find("b"), 20);
        assert_eq!(dict.find("c"), 30);
        assert_eq!(dict.find("z"), 40);
        assert_eq!(dict.find("d"), -1);
    }

    #[test]
    fn test_find_exact_block_boundary() {
        let mut dict = StringInMemoryDict::new(2);
        dict.add_term("first", 10, 1);
        dict.add_term("second", 20, 2);
        dict.add_term("third", 30, 3); // Block boundary
        dict.add_term("fourth", 40, 4);

        // Test finding terms that are exactly at block boundaries
        assert_eq!(dict.find("first"), 10); // First term in first block
        assert_eq!(dict.find("third"), 30); // First term in second block
    }
}

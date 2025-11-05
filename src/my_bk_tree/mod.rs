use bk_tree::{BKTree, metrics};

pub struct BkTree {
    bk_tree: BKTree<String>,
}

impl BkTree {
    pub fn new() -> Self {
        Self {
            bk_tree: BKTree::new(metrics::Levenshtein),
        }
    }

    pub fn find(&mut self,key:&str,edit_distance:u32)->Vec<String>{
        let mut result:Vec<String>=Vec::new();
        let result_words=self.bk_tree.find(key, edit_distance).collect::<Vec<_>>();
        for word in result_words{
            result.push(word.1.clone());
        }
        result
    }

    pub fn add(& mut self,key:&str){
        self.bk_tree.add(String::from(key));
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bk_tree() {
        let bk_tree = BkTree::new();
        // Just verify it can be created without panic
        assert!(true);
    }

    #[test]
    fn test_add_and_find_exact_match() {
        let mut bk_tree = BkTree::new();
        bk_tree.add("hello");
        
        let results = bk_tree.find("hello", 0);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "hello");
    }

    #[test]
    fn test_find_with_edit_distance() {
        let mut bk_tree = BkTree::new();
        bk_tree.add("hello");
        bk_tree.add("help");
        bk_tree.add("world");
        
        let results = bk_tree.find("helo", 1);
        assert!(results.contains(&"hello".to_string()));
        assert!(results.contains(&"help".to_string()));
        assert!(!results.contains(&"world".to_string()));
    }

    #[test]
    fn test_find_empty_tree() {
        let mut bk_tree = BkTree::new();
        let results = bk_tree.find("test", 2);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_add_multiple_words() {
        let mut bk_tree = BkTree::new();
        bk_tree.add("cat");
        bk_tree.add("dog");
        bk_tree.add("bat");
        
        let results = bk_tree.find("cat", 1);
        assert!(results.contains(&"cat".to_string()));
        assert!(results.contains(&"bat".to_string()));
        assert!(!results.contains(&"dog".to_string()));
    }
}
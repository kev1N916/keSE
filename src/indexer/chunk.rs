use crate::{
    compressors::vb_encode::vb_encode,
    indexer::helper::{vb_decode_positions, vb_encode_positions},
};
const POSITIONS_DELIMITER: u8 = 0x00;
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub size_of_chunk: u32, // stored on disk
    pub max_doc_id: u32,    // stored on disk
    pub doc_ids: Vec<u8>,   // stored on disk
    pub positions: Vec<u8>, // stored on disk
    pub no_of_postings: u8,
    pub term: u32,
    pub last_doc_id: u32,
}

impl Chunk {
    pub fn new(term: u32) -> Self {
        Self {
            size_of_chunk: 8,
            max_doc_id: 0,
            last_doc_id: 0,
            no_of_postings: 0,
            term: term,
            doc_ids: Vec::new(),
            positions: Vec::new(),
        }
    }

    pub fn finish(&mut self) {
        if self.doc_ids.len() > 0 {
            self.doc_ids.push(POSITIONS_DELIMITER);
            self.size_of_chunk+=1;
        }
    }
  
    pub fn reset(&mut self) {
        self.size_of_chunk = 8;
        self.last_doc_id = 0;
        self.max_doc_id = 0;
        self.positions.clear();
        self.doc_ids.clear();
        self.no_of_postings = 0;
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut chunk_bytes: Vec<u8> = Vec::new();
        chunk_bytes.extend_from_slice(&self.size_of_chunk.to_le_bytes());
        chunk_bytes.extend_from_slice(&self.max_doc_id.to_le_bytes());
        chunk_bytes.extend(&self.doc_ids);
        chunk_bytes.extend(&self.positions);
        chunk_bytes
    }

    pub fn get_posting_list(&mut self,index:u32)->Vec<u32>{
       
       let mut posting_list: &[u8] = &[];
        let mut current_index=0;
        let mut i=0;
        while current_index<index+1{
            let mut j=i;
            while self.positions[j]!=0{
                j+=1;
            }
            posting_list=&self.positions[i as usize..j as usize];
            i=j;
            current_index+=1;
        }
        vb_decode_positions(posting_list)
    }

    pub fn decode(&mut self, chunk_bytes: &[u8]) {
        self.size_of_chunk = (4 + chunk_bytes.len()) as u32;
        let mut offset = 0;
        let max_doc_id = u32::from_le_bytes(chunk_bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        self.max_doc_id = max_doc_id;
        if max_doc_id == 0 {
            return;
        }
        let mut index = offset;
        while index < chunk_bytes.len() {
            if chunk_bytes[index] == 0 {
                break;
            }
            index += 1;
        }
        self.doc_ids = chunk_bytes[offset..index].to_vec();
        self.positions = chunk_bytes[index..].to_vec();
    }
    pub fn add_encoded_doc_id(&mut self, doc_id: u32, encoded_doc_id: Vec<u8>) {
        self.last_doc_id = doc_id;
        self.size_of_chunk += encoded_doc_id.len() as u32;
        self.doc_ids.extend_from_slice(&encoded_doc_id);
    }
    pub fn encode_doc_id(&mut self, doc_id: u32) -> Vec<u8> {
        let encoded_doc_id: Vec<u8> = vb_encode(&(doc_id - self.last_doc_id));
        encoded_doc_id
    }
    pub fn add_encoded_positions(&mut self, encoded_positions: Vec<u8>) {
        self.size_of_chunk += encoded_positions.len() as u32;
        self.positions.extend_from_slice(&encoded_positions);
    }
    pub fn encode_positions(&mut self, positions: &Vec<u32>) -> Vec<u8> {
        let mut encoded_positions: Vec<u8> = vb_encode_positions(&positions);
        encoded_positions.push(POSITIONS_DELIMITER);
        encoded_positions
    }

    pub fn set_max_doc_id(&mut self, doc_id: u32) {
        let _ = self.max_doc_id.max(doc_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_chunk() {
        let chunk = Chunk::new(42);

        assert_eq!(chunk.term, 42);
        assert_eq!(chunk.size_of_chunk, 8);
        assert_eq!(chunk.max_doc_id, 0);
        assert_eq!(chunk.last_doc_id, 0);
        assert_eq!(chunk.no_of_postings, 0);
        assert!(chunk.doc_ids.is_empty());
        assert!(chunk.positions.is_empty());
    }

    #[test]
    fn test_reset() {
        let mut chunk = Chunk::new(1);

        // Modify the chunk
        chunk.size_of_chunk = 100;
        chunk.last_doc_id = 50;
        chunk.max_doc_id = 75;
        chunk.no_of_postings = 5;
        chunk.doc_ids.extend_from_slice(&[1, 2, 3]);
        chunk.positions.extend_from_slice(&[4, 5, 6]);

        // Reset and verify
        chunk.reset();

        assert_eq!(chunk.size_of_chunk, 8);
        assert_eq!(chunk.last_doc_id, 0);
        assert_eq!(chunk.max_doc_id, 0);
        assert_eq!(chunk.no_of_postings, 0);
        assert!(chunk.doc_ids.is_empty());
        assert!(chunk.positions.is_empty());
        assert_eq!(chunk.term, 1); // term should not change
    }

    #[test]
    fn test_add_encoded_doc_id() {
        let mut chunk = Chunk::new(1);
        let encoded = vec![0x85, 0x01]; // example encoded value

        chunk.add_encoded_doc_id(100, encoded.clone());

        assert_eq!(chunk.last_doc_id, 100);
        assert_eq!(chunk.size_of_chunk, 8 + encoded.len() as u32);
        assert_eq!(chunk.doc_ids, encoded);
    }

    #[test]
    fn test_add_multiple_encoded_doc_ids() {
        let mut chunk = Chunk::new(1);

        let encoded1 = vec![0x01];
        let encoded2 = vec![0x02, 0x03];

        chunk.add_encoded_doc_id(10, encoded1.clone());
        chunk.add_encoded_doc_id(20, encoded2.clone());

        assert_eq!(chunk.last_doc_id, 20);
        assert_eq!(chunk.size_of_chunk, 8 + 1 + 2);
        assert_eq!(chunk.doc_ids, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_encode_doc_id_first_document() {
        let mut chunk = Chunk::new(1);

        let encoded = chunk.encode_doc_id(100);

        // First doc_id is encoded as is (100 - 0)
        assert!(!encoded.is_empty());
        // The actual encoding depends on vb_encode implementation
    }

    #[test]
    fn test_encode_doc_id_subsequent_documents() {
        let mut chunk = Chunk::new(1);
        chunk.last_doc_id = 100;

        let encoded = chunk.encode_doc_id(150);
        let vb_encoded = vb_encode(&50);
        // Should encode the delta (150 - 100 = 50)
        assert_eq!(encoded, vb_encoded);
    }

    #[test]
    fn test_add_encoded_positions() {
        let mut chunk = Chunk::new(1);
        let encoded_pos = vec![0x10, 0x20, 0x30];

        chunk.add_encoded_positions(encoded_pos.clone());

        assert_eq!(chunk.size_of_chunk, 8 + encoded_pos.len() as u32);
        assert_eq!(chunk.positions, encoded_pos);
    }

    #[test]
    fn test_add_multiple_encoded_positions() {
        let mut chunk = Chunk::new(1);

        let pos1 = vec![0x01, 0x02];
        let pos2 = vec![0x03, 0x04, 0x05];

        chunk.add_encoded_positions(pos1.clone());
        chunk.add_encoded_positions(pos2.clone());

        assert_eq!(chunk.size_of_chunk, 8 + 2 + 3);
        assert_eq!(chunk.positions, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
    }

    #[test]
    fn test_encode_positions_empty() {
        let mut chunk = Chunk::new(1);
        let positions = vec![];

        let encoded = chunk.encode_positions(&positions);

        // Should still have delimiter
        assert_eq!(encoded.last(), Some(&POSITIONS_DELIMITER));
    }

    #[test]
    fn test_chunk_clone() {
        let mut chunk1 = Chunk::new(1);
        chunk1.doc_ids = vec![1, 2, 3];
        chunk1.positions = vec![4, 5, 6];
        chunk1.last_doc_id = 42;

        let chunk2 = chunk1.clone();

        assert_eq!(chunk1, chunk2);
        assert_eq!(chunk2.doc_ids, vec![1, 2, 3]);
        assert_eq!(chunk2.positions, vec![4, 5, 6]);
    }
}

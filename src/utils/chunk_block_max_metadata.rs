#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkBlockMaxMetadata {
    pub chunk_last_doc_id: u32,
    pub chunk_max_term_score: f32,
}

impl ChunkBlockMaxMetadata {
    pub fn new(chunk_last_doc_id: u32, chunk_max_term_score: f32) -> Self {
        Self {
            chunk_last_doc_id,
            chunk_max_term_score,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(8); // 4 bytes + 4 bytes

        // Write chunk_last_doc_id (4 bytes)
        buffer.extend_from_slice(&self.chunk_last_doc_id.to_le_bytes());

        // Write chunk_max_term_score (4 bytes)
        buffer.extend_from_slice(&self.chunk_max_term_score.to_le_bytes());

        buffer
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 {
            return Err("Not enough bytes to decode ChunkBlockMaxMetadata".to_string());
        }

        let chunk_last_doc_id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let chunk_max_term_score = f32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        Ok(Self {
            chunk_last_doc_id,
            chunk_max_term_score,
        })
    }
}

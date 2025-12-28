use crate::utils::chunk_block_max_metadata::ChunkBlockMaxMetadata;

#[derive(Debug, Clone, PartialEq)]
pub struct InMemoryTermMetadata<'a> {
    pub term_id: u32,
    pub term_frequency: u32,
    pub max_score: f32,
    pub block_ids: &'a [u32],
    pub chunk_block_max_metadata: Option<&'a Box<[ChunkBlockMaxMetadata]>>,
}

impl<'a> InMemoryTermMetadata<'a> {
    pub fn new(
        term_id: u32,
        block_ids: &'a [u32],
        chunk_block_max_metadata: &'a Box<[ChunkBlockMaxMetadata]>,
    ) -> Self {
        Self {
            term_id,
            term_frequency: 0,
            max_score: 0.0,
            block_ids,
            chunk_block_max_metadata: Some(chunk_block_max_metadata),
        }
    }
}
// pub fn encode(&self) -> Vec<u8> {
//     let mut buffer = Vec::new();

//     // Write term_id (4 bytes)
//     buffer.extend_from_slice(&self.term_id.to_le_bytes());

//     // Write term_frequency (4 bytes)
//     buffer.extend_from_slice(&self.term_frequency.to_le_bytes());

//     // Write max_score (4 bytes)
//     buffer.extend_from_slice(&self.max_score.to_le_bytes());

//     // Write number of block_ids (4 bytes)
//     buffer.extend_from_slice(&(self.block_ids.len() as u32).to_le_bytes());

//     // Write each block_id (4 bytes each)
//     for &block_id in &self.block_ids {
//         buffer.extend_from_slice(&block_id.to_le_bytes());
//     }

//     // Write number of chunk_block_max_metadata entries (4 bytes)
//     buffer.extend_from_slice(&(self.chunk_block_max_metadata.len() as u32).to_le_bytes());

//     // Write each ChunkBlockMaxMetadata (8 bytes each)
//     for metadata in &self.chunk_block_max_metadata {
//         buffer.extend_from_slice(&metadata.encode());
//     }

//     buffer
// }

// pub fn decode(bytes: &[u8]) -> Result<Self, String> {
//     use std::io::{Cursor, Read};

//     let mut cursor = Cursor::new(bytes);
//     let mut buf = [0u8; 4];

//     // Read term_id (4 bytes)
//     cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
//     let term_id = u32::from_le_bytes(buf);

//     // Read term_frequency (4 bytes)
//     cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
//     let term_frequency = u32::from_le_bytes(buf);

//     // Read max_score (4 bytes)
//     cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
//     let max_score = f32::from_le_bytes(buf);

//     // Read number of block_ids (4 bytes)
//     cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
//     let num_block_ids = u32::from_le_bytes(buf) as usize;

//     // Read block_ids
//     let mut block_ids = Vec::with_capacity(num_block_ids);
//     for _ in 0..num_block_ids {
//         cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
//         block_ids.push(u32::from_le_bytes(buf));
//     }

//     // Read number of chunk_block_max_metadata entries (4 bytes)
//     cursor.read_exact(&mut buf).map_err(|e| e.to_string())?;
//     let num_metadata = u32::from_le_bytes(buf) as usize;

//     // Read chunk_block_max_metadata
//     let mut chunk_block_max_metadata = Vec::with_capacity(num_metadata);
//     let mut metadata_buf = [0u8; 8];
//     for _ in 0..num_metadata {
//         cursor
//             .read_exact(&mut metadata_buf)
//             .map_err(|e| e.to_string())?;
//         chunk_block_max_metadata.push(ChunkBlockMaxMetadata::decode(&metadata_buf)?);
//     }

//     Ok(Self {
//         term_id,
//         term_frequency,
//         max_score,
//         block_ids,
//         chunk_block_max_metadata,
//     })
// }

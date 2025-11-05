use crate::{compressors::vb_encode::{vb_decode, vb_encode}, dictionary::Posting};


pub (crate) fn vb_decode_positions(bytes: &[u8]) -> Vec<u32> {
    let mut positions = Vec::new();
    let mut offset = 0;
    let mut last_position = 0;
    while offset < bytes.len() {
        let (position, bytes_read) = vb_decode(&bytes[offset..]);
        if bytes_read == 0 {
            break;
        }
        if last_position == 0 {
            positions.push(position);
            last_position = position;
        } else {
            positions.push(last_position + position);
            last_position = last_position + position;
        }
        offset += bytes_read;
    }

    positions
}

pub (crate) fn vb_encode_positions(positions: &Vec<u32>) -> Vec<u8> {
    let mut vb_encoded_positions = Vec::<u8>::new();
    let mut last_position = 0;
    for position in positions {
        if last_position == 0 {
            let mut bytes = vb_encode(position);
            vb_encoded_positions.append(&mut bytes);
            last_position = *position
        } else {
            let position_difference = position - last_position;
            let mut bytes = vb_encode(&position_difference);
            vb_encoded_positions.append(&mut bytes);
            last_position = *position
        }
    }
    vb_encoded_positions
}

pub (crate) fn vb_decode_posting_list(encoded_bytes: &[u8]) -> Vec<Posting> {
    let mut posting_list: Vec<Posting> = Vec::new();
    let mut offset = 0;
    let mut last_doc_id = 0;

    while offset < encoded_bytes.len() {
        // Decode doc_id (or doc_id_difference after first posting)
        let (doc_id_raw, bytes_read) = vb_decode(&encoded_bytes[offset..]);
        offset += bytes_read;

        // Calculate actual doc_id
        let doc_id = if last_doc_id == 0 {
            doc_id_raw // First posting uses absolute doc_id
        } else {
            last_doc_id + doc_id_raw // Subsequent postings use difference
        };

        // Read positions length (2 bytes, little endian)
        if offset + 2 > encoded_bytes.len() {
            break; // Not enough bytes for length
        }
        let positions_length =
            u16::from_le_bytes([encoded_bytes[offset], encoded_bytes[offset + 1]]) as usize;
        offset += 2;

        // Read and decode positions
        if offset + positions_length > encoded_bytes.len() {
            break; // Not enough bytes for positions
        }
        let positions = vb_decode_positions(&encoded_bytes[offset..offset + positions_length]);
        offset += positions_length;

        // Create posting and add to list
        posting_list.push(Posting { doc_id, positions });

        last_doc_id = doc_id;
    }

    posting_list
}

pub (crate) fn vb_encode_posting_list(posting_list: &Vec<Posting>) -> Vec<u8> {
    let mut posting_list_bytes: Vec<u8> = Vec::<u8>::new();
    let mut last_doc_id = 0;
    for posting in posting_list {
        if last_doc_id == 0 {
            let mut posting_bytes = vb_encode(&posting.doc_id);
            let mut position_bytes = vb_encode_positions(&posting.positions);
            posting_list_bytes.append(&mut posting_bytes);
            let positions_length: u16 = position_bytes.len() as u16;
            let mut length_bytes: Vec<u8> = positions_length.to_le_bytes().to_vec();
            posting_list_bytes.append(&mut length_bytes);
            posting_list_bytes.append(&mut position_bytes);
        } else {
            let doc_id_difference = posting.doc_id - last_doc_id;
            let mut posting_bytes = vb_encode(&doc_id_difference);
            let mut position_bytes = vb_encode_positions(&posting.positions);
            posting_list_bytes.append(&mut posting_bytes);
            let positions_length: u16 = position_bytes.len() as u16;
            let mut length_bytes: Vec<u8> = positions_length.to_le_bytes().to_vec();
            posting_list_bytes.append(&mut length_bytes);
            posting_list_bytes.append(&mut position_bytes);
        }
        last_doc_id = posting.doc_id
    }

    posting_list_bytes
}

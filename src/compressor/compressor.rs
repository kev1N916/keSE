use search_engine_compressors::*;

#[derive(Debug, Clone, PartialEq)]
pub enum CompressionAlgorithm {
    Simple9,
    Simple16,
    PforDelta,
    RiceCoding,
    VarByte,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Compressor {
    compression_algorithm: CompressionAlgorithm,
}

pub fn transform_list_for_difference_encoding(list: &Vec<u32>) -> Vec<u32> {
    let mut list_with_gaps = Vec::new();
    let mut last_member = 0;
    for member in list {
        if last_member == 0 {
            list_with_gaps.push(*member);
            last_member = *member
        } else {
            let difference = *member - last_member;
            list_with_gaps.push(difference);
            last_member = *member
        }
    }
    list_with_gaps
}

pub fn transform_list_to_difference_encoding(list_with_gaps: Vec<u32>) -> Vec<u32> {
    let mut list_without_gaps = Vec::new();

    let mut last_member = 0;
    for member in list_with_gaps {
        if last_member == 0 {
            list_without_gaps.push(member);
            last_member = member;
        } else {
            last_member += member;
            list_without_gaps.push(last_member);
        }
    }
    list_without_gaps
}

impl Compressor {
    pub fn new(compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            compression_algorithm,
        }
    }

    pub fn compress_list_with_difference(&self, list: &Vec<u32>) -> Vec<u8> {
        match self.compression_algorithm {
            CompressionAlgorithm::Simple9 => {
                return simple9::compress(&transform_list_for_difference_encoding(list));
            }
            CompressionAlgorithm::Simple16 => {
                return simple16::compress(&transform_list_for_difference_encoding(list));
            }
            CompressionAlgorithm::PforDelta => {
                return p_for_delta::compress(&transform_list_for_difference_encoding(list));
            }
            CompressionAlgorithm::RiceCoding => {
                return rice::compress(&transform_list_for_difference_encoding(list), None);
            }
            CompressionAlgorithm::VarByte => {
                return var_byte::compress(&transform_list_for_difference_encoding(list));
            }
        }
    }

    pub fn decompress_list_with_difference(&self, list: &Vec<u8>) -> Vec<u32> {
        match self.compression_algorithm {
            CompressionAlgorithm::Simple9 => {
                return transform_list_to_difference_encoding(simple9::decompress_from_bytes(list));
            }
            CompressionAlgorithm::Simple16 => {
                return transform_list_to_difference_encoding(simple16::decompress_from_bytes(
                    list,
                ));
            }
            CompressionAlgorithm::PforDelta => {
                return transform_list_to_difference_encoding(p_for_delta::decompress(list));
            }
            CompressionAlgorithm::RiceCoding => {
                // return transform_list_to_difference_encoding(rice::decompress(list));
                Vec::new()
            }
            CompressionAlgorithm::VarByte => {
                return transform_list_to_difference_encoding(var_byte::decompress(list));
            }
        }
    }

    pub fn compress_list(&self, list: &Vec<u32>) -> Vec<u8> {
        match self.compression_algorithm {
            CompressionAlgorithm::Simple9 => {
                return simple9::compress(&list);
            }
            CompressionAlgorithm::Simple16 => {
                return simple16::compress(&list);
            }
            CompressionAlgorithm::PforDelta => {
                return p_for_delta::compress(&(list));
            }
            CompressionAlgorithm::RiceCoding => {
                return rice::compress(&(list), None);
            }
            CompressionAlgorithm::VarByte => {
                return var_byte::compress(&(list));
            }
        }
    }

    pub fn decompress_list(&self, list: &Vec<u8>) -> Vec<u32> {
        match self.compression_algorithm {
            CompressionAlgorithm::Simple9 => {
                return simple9::decompress_from_bytes(list);
            }
            CompressionAlgorithm::Simple16 => {
                return simple16::decompress_from_bytes(list);
            }
            CompressionAlgorithm::PforDelta => {
                return p_for_delta::decompress(list);
            }
            CompressionAlgorithm::RiceCoding => {
                // return transform_list_to_difference_encoding(rice::decompress(list));
                Vec::new()
            }
            CompressionAlgorithm::VarByte => {
                return var_byte::decompress(list);
            }
        }
    }
}

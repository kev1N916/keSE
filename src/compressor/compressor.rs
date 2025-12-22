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

impl Compressor {
    pub fn new(compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            compression_algorithm,
        }
    }

    fn transform_list_for_d_gap_encoding(list: &Vec<u32>) -> Vec<u32> {
        let mut list_with_gaps = Vec::with_capacity(128);
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

    fn reconstruct_list_from_d_gap_encoding(list_with_gaps: Vec<u32>) -> Vec<u32> {
        println!("{:?}", list_with_gaps);
        let mut list_without_gaps = Vec::with_capacity(128);

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

    pub fn compress_list_with_d_gaps(&self, list: &Vec<u32>) -> Vec<u8> {
        match self.compression_algorithm {
            CompressionAlgorithm::Simple9 => {
                return simple9::compress(&Self::transform_list_for_d_gap_encoding(list));
            }
            CompressionAlgorithm::Simple16 => {
                return simple16::compress(&Self::transform_list_for_d_gap_encoding(list));
            }
            CompressionAlgorithm::PforDelta => {
                let mut list_with_differences = Self::transform_list_for_d_gap_encoding(list);
                if list_with_differences.len() < 128 {
                    list_with_differences.reserve(128 - list_with_differences.len());
                    for _ in 0..128 - list_with_differences.len() {
                        list_with_differences.push(0);
                    }
                }
                return p_for_delta::compress(&list_with_differences);
            }
            CompressionAlgorithm::RiceCoding => {
                return rice::compress(&Self::transform_list_for_d_gap_encoding(list), None);
            }
            CompressionAlgorithm::VarByte => {
                return var_byte::compress(&Self::transform_list_for_d_gap_encoding(list));
            }
        }
    }

    pub fn decompress_list_with_dgaps(&self, list: &Vec<u8>) -> Vec<u32> {
        match self.compression_algorithm {
            CompressionAlgorithm::Simple9 => {
                return Self::reconstruct_list_from_d_gap_encoding(simple9::decompress_from_bytes(
                    list,
                ));
            }
            CompressionAlgorithm::Simple16 => {
                return Self::reconstruct_list_from_d_gap_encoding(
                    simple16::decompress_from_bytes(list),
                );
            }
            CompressionAlgorithm::PforDelta => {
                let list = p_for_delta::decompress(list);
                let mut index = 0;
                while index < list.len() {
                    if list[index] == 0 {
                        break;
                    }
                    index += 1;
                }
                return Self::reconstruct_list_from_d_gap_encoding(list[0..index].to_vec());
            }
            CompressionAlgorithm::RiceCoding => {
                // return transform_list_to_difference_encoding(rice::decompress(list));
                Vec::new()
            }
            CompressionAlgorithm::VarByte => {
                return Self::reconstruct_list_from_d_gap_encoding(var_byte::decompress(list));
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
                let mut p_for_delta_vec = vec![0; 128];
                for i in 0..list.len() {
                    p_for_delta_vec[i] = list[i];
                }
                return p_for_delta::compress(&p_for_delta_vec);
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
                let list = p_for_delta::decompress(list);
                let mut index = 0;
                while index < list.len() {
                    if list[index] == 0 {
                        break;
                    }
                    index += 1;
                }
                list[0..index].to_vec()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_p_for_delta_compressor() {
        let compressor = Compressor::new(CompressionAlgorithm::PforDelta);
        let data = vec![1, 4, 6, 13, 7, 128, 68, 70, 326, 34];
        let bytes = compressor.compress_list(&data);
        let decoded = compressor.decompress_list(&bytes);
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_p_for_delta_compressor_with_d_gap() {
        let compressor = Compressor::new(CompressionAlgorithm::PforDelta);
        let data = vec![1, 4, 6, 13, 89, 128, 681, 702, 3263, 3489];
        let bytes = compressor.compress_list_with_d_gaps(&data);
        let decoded = compressor.decompress_list_with_dgaps(&bytes);
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_simple_16_compressor_with_d_gap() {
        let compressor = Compressor::new(CompressionAlgorithm::Simple16);
        let data = vec![1, 4, 6, 13, 89, 128, 681, 702, 3263, 3489];
        let bytes = compressor.compress_list_with_d_gaps(&data);
        let decoded = compressor.decompress_list_with_dgaps(&bytes);
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_simple_9_compressor_with_d_gap() {
        let compressor = Compressor::new(CompressionAlgorithm::Simple9);
        let data = vec![1, 4, 6, 13, 89, 128, 681, 702, 3263, 3489];
        let bytes = compressor.compress_list(&data);
        let decoded = compressor.decompress_list(&bytes);
        assert_eq!(data, decoded);
    }
}

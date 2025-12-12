pub fn vb_encode(num: &u32) -> Vec<u8> {
    let mut mutable_num = num.clone();
    let mut bytes = Vec::<u8>::new();
    loop {
        let byte = (mutable_num % 128) as u8;
        bytes.insert(0, byte);
        if mutable_num < 128 {
            break;
        }
        mutable_num = mutable_num / 128
    }
    let bytes_length = bytes.len();
    bytes[bytes_length - 1] += 128;

    bytes
}

pub fn vb_decode(bytes: &[u8]) -> (u32, usize) {
    let mut number: u32 = 0;
    let mut bytes_read = 0;
    for byte in bytes {
        bytes_read = bytes_read + 1;
        if *byte < 128 {
            number = number * 128 + (*byte) as u32;
        } else {
            number = number * 128 + ((*byte) as u32 - 128);
            break;
        }
    }
    (number, bytes_read)
}

#[cfg(test)]
mod vb_decode_tests {
    use super::*;

    #[test]
    fn test_1097() {
        let (number, bytes_read) = vb_decode(&vec![8, 201]);
        assert_eq!(number, 1097);
        assert_eq!(bytes_read, 2);
    }
}

#[cfg(test)]
mod vb_encodee_tests {
    use super::*;

    #[test]
    fn test_zero() {
        let num = 0;
        let result = vb_encode(&num);
        assert_eq!(result, vec![128]); // 0 + 128 (continuation bit)
    }

    #[test]
    fn test_one() {
        let num = 1;
        let result = vb_encode(&num);
        assert_eq!(result, vec![129]); // 1 + 128 (continuation bit)
    }

    #[test]
    fn test_small_numbers() {
        let test_cases = vec![
            (5, vec![133]),   // 5 + 128
            (42, vec![170]),  // 42 + 128
            (100, vec![228]), // 100 + 128
            (127, vec![255]), // 127 + 128
        ];

        for (input, expected) in test_cases {
            let num = input;
            let result = vb_encode(&num);
            assert_eq!(result, expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_1097() {
        let num = 1097;
        let result = vb_encode(&num);
        // 128 = 1 * 128 + 0
        // First iteration: byte = 0, num becomes 1
        // Second iteration: byte = 1, num < 128, break
        // Result: [1, 0], then add 128 to last element: [1, 128]
        assert_eq!(result, vec![8, 201]);
    }

    #[test]
    fn test_powers_of_128() {
        let test_cases = vec![
            (128, vec![1, 128]),           // 128^1
            (16384, vec![1, 0, 128]),      // 128^2 = 16384
            (2097152, vec![1, 0, 0, 128]), // 128^3 = 2097152
        ];

        for (input, expected) in test_cases {
            let num = input;
            let result = vb_encode(&num);
            assert_eq!(result, expected, "Failed for power of 128: {}", input);
        }
    }

    #[test]
    fn test_edge_case_127() {
        let num = 127; // Largest single-byte number
        let result = vb_encode(&num);
        assert_eq!(result, vec![255]); // 127 + 128 = 255
    }
}

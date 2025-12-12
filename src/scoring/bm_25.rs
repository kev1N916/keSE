/// BM25 scoring implementation
///
/// Formula:
/// BM25 = Σ(t∈q) log((N - f_t + 0.5) / (f_t + 0.5)) * TF_BM25
///
/// where TF_BM25 = (f_t,d * (k1 + 1)) / (f_t,d + k1 * ((1 - b) + (b * ℓ_d / ℓ_avg)))

/// BM25 parameters
pub struct BM25Params {
    pub k1: f32, // Term frequency saturation parameter (typical: 1.2)
    pub b: f32,  // Length normalization parameter (typical: 0.75)
}

impl Default for BM25Params {
    fn default() -> Self {
        BM25Params { k1: 1.2, b: 0.75 }
    }
}

/// Compute IDF component for a term
/// IDF = log((N - f_t + 0.5) / (f_t + 0.5))
///
/// # Arguments
/// * `n` - Total number of documents (N)
/// * `f_t` - Number of documents containing term t (document frequency)
pub fn compute_idf(n: u32, f_t: u32) -> f32 {
    let n = n as f32;
    let f_t = f_t as f32;

    ((n - f_t + 0.5) / (f_t + 0.5)).ln()
}

/// Compute TF component for BM25
/// TF_BM25 = (f_t,d * (k1 + 1)) / (f_t,d + k1 * ((1 - b) + (b * ℓ_d / ℓ_avg)))
///
/// # Arguments
/// * `f_td` - Term frequency in document (f_t,d)
/// * `doc_len` - Document length (ℓ_d)
/// * `avg_doc_len` - Average document length (ℓ_avg)
/// * `params` - BM25 parameters (k1, b)
pub fn compute_tf_bm25(f_td: u32, doc_len: u32, avg_doc_len: f32, params: &BM25Params) -> f32 {
    let f_td = f_td as f32;
    let doc_len = doc_len as f32;
    let k1 = params.k1;
    let b = params.b;

    let numerator = f_td * (k1 + 1.0);
    let denominator = f_td + k1 * ((1.0 - b) + (b * doc_len / avg_doc_len));

    numerator / denominator
}

/// Compute BM25 score for a single term in a document
pub fn compute_term_score(
    f_td: u32,        // Term frequency in document
    doc_len: u32,     // Document length
    avg_doc_len: f32, // Average document length
    n: u32,           // Total number of documents
    f_t: u32,         // Document frequency of term
    params: &BM25Params,
) -> f32 {
    let idf = compute_idf(n, f_t);
    let tf = compute_tf_bm25(f_td, doc_len, avg_doc_len, params);

    idf * tf
}

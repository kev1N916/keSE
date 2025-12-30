#[derive(Clone, Debug)]
pub struct DocumentMetadata {
    pub doc_name: String,
    pub doc_url: String,
    pub doc_length: u32,
}

#[derive(Clone, Debug)]
pub struct SearchEngineMetadata {
    pub no_of_docs: u32,
    pub no_of_terms: u32,
    pub no_of_blocks: u32,
    pub size_of_index: f64,
    pub dataset_directory_path: String,
    pub index_directory_path: String,
    pub compression_algorithm: String,
    pub query_algorithm: String,
}

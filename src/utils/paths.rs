use std::path::{Path, PathBuf};

pub fn get_save_term_metadata_path<P: AsRef<Path>>(path: P) -> PathBuf {
    path.as_ref().join("term_metadata.sidx")
}

pub fn get_save_doc_metadata_path<P: AsRef<Path>>(path: P) -> PathBuf {
    path.as_ref().join("document_metadata.sidx")
}
pub fn get_inverted_index_path<P: AsRef<Path>>(path: P) -> PathBuf {
    path.as_ref().join("inverted_index.idx")
}

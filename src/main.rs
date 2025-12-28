use mimalloc::MiMalloc;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::compressor::compressor::CompressionAlgorithm;
use crate::query_processor::retrieval_algorithms::QueryAlgorithm;
use crate::search_engine::search_engine::SearchEngine;

mod compressor;
mod in_memory_index_metadata;
mod indexer;
mod query_parser;
mod scoring;
mod search_engine;
mod utils;

mod query_processor;
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Config {
    result_dir: String,
    index_dir: String,
    query_algo: String,
    compression_algo: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            result_dir: "./results".to_string(),
            index_dir: "./index".to_string(),
            query_algo: "wand".to_string(),
            compression_algo: "simple16".to_string(),
        }
    }
}

fn load_config(path: &str) -> Config {
    if Path::new(path).exists() {
        match fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str(&contents) {
                Ok(config) => {
                    println!("✓ Loaded configuration from {}", path);
                    return config;
                }
                Err(e) => {
                    eprintln!("⚠ Error parsing config.json: {}", e);
                    eprintln!("  Using default configuration");
                }
            },
            Err(e) => {
                eprintln!("⚠ Error reading config.json: {}", e);
                eprintln!("  Using default configuration");
            }
        }
    } else {
        println!("ℹ config.json not found, using default configuration");
    }

    Config::default()
}

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let mut rl = DefaultEditor::new().unwrap();

    // Load configuration from config.json
    let config_path = "config.json";
    let mut config = load_config(config_path);

    println!("\nCurrent Configuration:");
    println!("  Result Directory:      {}", config.result_dir);
    println!("  Index Directory:       {}", config.index_dir);
    println!("  Query Algorithm:       {}", config.query_algo);
    println!("  Compression Algorithm: {}", config.compression_algo);
    println!("\nWelcome to my CLI! Type 'help' for commands or 'exit' to quit.\n");
    let compression_algo = match config.compression_algo.as_str() {
        "varbyte" => CompressionAlgorithm::VarByte,
        "simple9" => CompressionAlgorithm::Simple9,
        "simple16" => CompressionAlgorithm::Simple16,
        "pfordelta" => CompressionAlgorithm::PforDelta,
        _ => CompressionAlgorithm::Simple16,
    };

    let query_algo = match config.query_algo.as_str() {
        "boolean" => QueryAlgorithm::Boolean,
        "bmw" => QueryAlgorithm::BlockMaxWand,
        "bmms" => QueryAlgorithm::BlockMaxMaxScore,
        "wand" => QueryAlgorithm::Wand,
        "ms" => QueryAlgorithm::MaxScore,
        _ => QueryAlgorithm::Wand,
    };
    let mut search_engine = SearchEngine::new(
        config.index_dir.clone(),
        compression_algo,
        query_algo,
        config.result_dir.clone(),
    );
    loop {
        let readline = rl.readline("> ");

        match readline {
            Ok(line) => {
                let line = line.trim();

                if line.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = line.split_whitespace().collect();
                let command = parts[0];

                match command {
                    _ => {}
                }
            }
            Err(_) => {}
        }
    }
}

use mimalloc::MiMalloc;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::time::SystemTime;

use crate::compressor::compressor::CompressionAlgorithm;
use crate::query_processor::retrieval_algorithms::QueryAlgorithm;
use crate::search_engine::search_engine::SearchEngine;

mod compressor;
mod in_memory_index_metadata;
mod indexer;
mod parser;
mod scoring;
mod search_engine;
mod utils;

mod query_processor;
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Config {
    index_dir: String,
    dataset_dir: String,
    query_algo: String,
    compression_algo: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            index_dir: "index".to_string(),
            dataset_dir: "wikipedia".to_string(),
            query_algo: "wand".to_string(),
            compression_algo: "simple16".to_string(),
        }
    }
}

fn load_config(path: &str) -> Config {
    if Path::new(path).exists() {
        println!("{:?}", path);
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

    let config_path = "config.json";
    let config = load_config(config_path);

    println!("\nCurrent Configuration:");
    println!("  Index Directory:      {}", config.index_dir);
    println!("  Dataset Directory:       {}", config.dataset_dir);
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
        config.dataset_dir,
        compression_algo,
        query_algo,
        config.index_dir,
    )
    .unwrap();
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
                    "help" => {
                        println!("The valid commands are->");
                        println!("index: Starts building your index ");
                        println!("save: Saves your index if it has already been built");
                        println!("load: Loads your previously saved index");
                        println!(
                            "query [query string]: Queries your index for the particular query string entered"
                        );
                    }
                    "index" => {
                        search_engine.build_index().unwrap();
                        println!("The index has been built")
                    }
                    "merge" => {
                        search_engine.merge_spimi_files().unwrap();
                        println!("The index has been built")
                    }
                    "terms" => {
                        let terms = search_engine.get_terms();
                        let mut max_length = 0;
                        for term in terms {
                            if term.len() <= 20 {
                                println!("{}", term);
                            }
                        }
                        println!("{}", max_length);
                    }
                    "metadata" => {
                        let metadata = search_engine.get_index_metadata();
                        println!(
                            "The size of the inverted index is {:?}",
                            metadata.size_of_index
                        );
                        println!(
                            "The number of indexed documents is {:?}",
                            metadata.no_of_docs
                        );
                        println!(
                            "The number of terms in the index is {:?}",
                            metadata.no_of_terms
                        );
                        println!(
                            "The number of blocks occupied by the index is {:?}",
                            metadata.no_of_blocks
                        );
                        println!(
                            "The compression algorithm used by the index is {:?}",
                            metadata.compression_algorithm
                        );
                        println!(
                            "The query algorithm used by the index is {:?}",
                            metadata.query_algorithm
                        );
                        println!(
                            "The index directory path is {:?}",
                            metadata.dataset_directory_path
                        );
                        println!(
                            "The index directory path is {:?}",
                            metadata.index_directory_path
                        );
                    }
                    "save" => {
                        search_engine.save_index().unwrap();
                        println!("The index has been saved successfully")
                    }
                    "load" => {
                        let start_time = SystemTime::now();
                        search_engine.load_index().unwrap();
                        let end_time = SystemTime::now();
                        // println!("{:?}", end_time.duration_since(start_time).unwrap());
                        println!(
                            "The index has been successfully loaded in {} seconds",
                            end_time.duration_since(start_time).unwrap().as_secs()
                        );
                    }
                    "query" => {
                        let query_string = parts[1..].join(" ");
                        let query_results = search_engine.handle_query(query_string).unwrap();
                        for i in (0..query_results.len()).rev() {
                            println!(
                                "{} {} score {}",
                                query_results[i].0.doc_name,
                                query_results[i].0.doc_url,
                                query_results[i].1
                            )
                        }
                    }
                    "quit" | "exit" => {
                        println!("Goodbye!");
                        break;
                    }
                    _ => {
                        println!(
                            "Invalid command. Type help if you want to see the valid commands"
                        );
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}

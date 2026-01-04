# keSE

**keSE** is a search engine that performs **SPIMI (Single Pass In-Memory Indexing)** to build an inverted index. It supports multiple ranked retrieval algorithms for querying and various document-ID compression algorithms during index construction.

---

## What is an Inverted Index?

At the heart of keSE is an **Inverted Index**. While a "forward index" maps documents to the words they contain (like a table of contents), an inverted index maps each unique term to a list of document IDs (postings) where that term appears.

This structure enables lightning-fast lookups. Instead of scanning every document for a keyword, the engine simply jumps to the term in the index and retrieves its associated list of documents.

---
# Information Retrieval Resources

A collection of foundational and advanced research papers covering inverted indexes, compression techniques, caching strategies, and query optimization.

## 📖 Overviews and Fundamentals
* **An Introduction to Information Retrieval** – *Manning et al.*
* **Efficient Query Processing for Scalable Web Search** – [Read Online](https://dl.acm.org/doi/abs/10.1561/1500000057)

## 🗜️ Compression Techniques
* **Inverted Index Compression and Query Processing with Optimized Document Ordering** – [Read Online](https://dl.acm.org/doi/10.1145/1526709.1526764)
* **Performance of Compressed Inverted List Caching in Search Engines** – [Read Online](https://dl.acm.org/doi/10.1145/1367497.1367550)
* **Compressing Term Positions in Web Indexes** – [Read Online](https://dl.acm.org/doi/10.1145/1571941.1571969)
* **Index Compression through Document Reordering** – [Read Online](https://dl.acm.org/doi/10.5555/882455.875020)

## ⚡ Caching Strategies
* **Improved Techniques for Result Caching in Web Search Engines** – [Read Online](https://dl.acm.org/doi/10.1145/1526709.1526768)
* **Rank-Preserving Two-Level Caching for Scalable Search Engines** – [Read Online](https://dl.acm.org/doi/10.1145/383952.383959)
* **On-Line File Caching** – [arXiv:cs/0205033](https://arxiv.org/abs/cs/0205033)

## 🔍 Query Processing & Optimization
* **Faster Top-k Document Retrieval Using Block-Max Indexes** – [Read Online](https://dl.acm.org/doi/10.1145/2009916.2010048)
* **MWAND: A New Early Termination Algorithm for Fast and Efficient Query Evaluation** – [ResearchGate](https://www.researchgate.net/publication/333435122_MWAND_A_New_Early_Termination_Algorithm_for_Fast_and_Efficient_Query_Evaluation)
* **Optimization Strategies for Complex Queries** – [Read Online](https://dl.acm.org/doi/10.1145/1076034.1076074)
* **Faster BlockMax WAND with Longer Skipping** – [Read Online](https://dl.acm.org/doi/10.1007/978-3-030-15712-8_52)

## 📂 Document Reordering
* **Document Reordering for Faster Intersection** – [Read Online](https://dl.acm.org/doi/10.14778/3303753.3303755)
* **Inverted File Compression Through Document Identifier Reassignment** – [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306457302000201)
  
## Why use SPIMI?

Building an index for millions of documents often exceeds available RAM. SPIMI is a highly efficient indexing algorithm that addresses this through:

- **Memory Efficiency**: Processes documents one by one, adding terms to an in-memory dictionary
- **No Sorting Required**: Unlike traditional sort-based indexing, SPIMI collects postings directly into lists. When memory is full, it sorts the dictionary and writes that "block" to disk
- **Scalability**: Once all blocks are written, they are merged into one final index. This "single pass" approach avoids the overhead of maintaining a massive global term-to-ID mapping in memory

---

## The Necessity of Doc-ID Compression

In a large-scale index, postings lists (the lists of document IDs) consume the most space. We compress these IDs for two primary reasons:

1. **Space Efficiency**: Storing raw 32-bit or 64-bit integers for every occurrence of a word is incredibly wasteful
2. **Increased Speed (I/O)**: Modern CPUs are much faster than disk I/O. By compressing the data, we reduce the amount that needs to be read from disk. Decompressing in RAM is significantly faster than reading uncompressed, bulky files from the drive

### How we compress: Delta Encoding (d-gaps)

Instead of storing absolute document IDs (e.g., `[100, 105, 110]`), we store the gaps between them (e.g., `[100, 5, 5]`). Since these gaps are much smaller numbers, they can be represented using fewer bits through algorithms like Variable Byte Encoding or Simple-16 Encoding.

---

## Supported Algorithms

keSE allows users to toggle between different strategies for index compression and document retrieval depending on performance requirements (e.g., speed vs. storage space).

### Index Compression Algorithms

| Algorithm | Type | Description |
|-----------|------|-------------|
| **VarByte** | Byte-aligned | Extremely fast decoding; uses a "continuation bit" to signal the end of a number |
| **Simple-9** | Bit-packing | Packs multiple small integers into a single 32-bit word using 9 fixed-bit patterns |
| **Simple-16** | Bit-packing | An optimized version of Simple-9 that uses 16 patterns to utilize 32-bit words more efficiently |
| **PforDelta** | Frame-of-Reference | Compresses most values in a "frame" using a small number of bits, while handling outliers as "exceptions" |
| **Rice Coding** | Entropy-based | Uses a quotient and remainder approach; highly effective for data following a geometric distribution |

### Retrieval & Ranking Algorithms

keSE implements dynamic pruning techniques while serving queries. These allow the engine to find the top-k documents without evaluating every single document in the postings list.

#### Traditional Retrieval

- **Boolean Retrieval**: The baseline model for exact matches using AND, OR, and NOT logic

#### Early Termination & Dynamic Pruning

These algorithms significantly speed up queries by "skipping" documents that cannot mathematically enter the top-k results:

- **WAND (Weak AND)**: Uses an upper-bound score to skip documents that don't meet a specific threshold
- **Max Score**: Partitions the query terms into "essential" and "optional" groups based on their maximum possible contribution to the final score
- **Block Max WAND**: An optimized WAND that uses block-level metadata to skip entire chunks of the index at once
- **Block Max Max Score**: Combines the strategy of Max Score with block-level score bounds for even tighter pruning

---

## Dataset

Our current inverted index is built on a processed Wikipedia dump:
```
https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
```

The data is cleaned using a Python script which compresses it in batches into zstd files. The compressed size of the Wikipedia dump is **24.8 GB**.
After cleaning the Wikipedia dump we are left with about **7.1 million** documents and about **18 million** unique terms(words).
However, the search engine can work on any type of data as long as it is cleaned.

The script used is present in the folder **python_wikipedia** .

---

## How to Use

A CLI is used to interact with the search engine. The CLI needs configuration information provided in a `config.json` file. This file should be placed in the base folder (the same folder with the README and Cargo.toml files).

### Configuration File (`config.json`)

```json
{
  "index_dir": "The resultant directory into which your index is going to be built", 
  "dataset_dir": "The directory which contains the dataset on which your index is going to be built",
  "compression_algo": "The compression algorithm you want your search engine to use",
  "query_algo": "The query algorithm you want your search engine to use"
}
```

---

## Commands

| Command | Description |
|---------|-------------|
| `index` | Starts the SPIMI process and builds your index into the directory you have provided. The resultant index file is `inverted_index.idx` |
| `save` | The index needs metadata to serve queries. This command saves metadata in the index directory so you can reuse your inverted index when you restart the CLI |
| `load` | Loads your saved metadata so you can start querying the index again |
| `metadata` | Produces metadata about your index like the size of the index, the number of terms and documents in the index, etc. |
| `query [QUERY STRING]` | Queries your inverted index for the `[QUERY STRING]` and returns the most relevant documents |
| `quit` | Exits the CLI. If you haven't saved the index metadata, you will have to rebuild the index from scratch next time |

---

## Getting Started

1. Create a `config.json` file with your configuration
2. Run `index` to build your inverted index
3. Use `save` to persist metadata
4. Query your index with `query [your search terms]`
5. Use `quit` to exit (remember to save first!)

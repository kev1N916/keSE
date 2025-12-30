keSE

keSE is a search engine which performs SPIMI(SINGLE PASS IN MEMORY INDEXING) to build an inverted index and 
allows for multiple ranked retrieval algorithms to be used while querying the index
as well as multiple document-id compression algorithms to be used while building the inverted index.

What is an Inverted Index ?

At the heart of keSE is an Inverted Index. While a "forward index" maps documents to the words they contain (like a Table of Contents), an inverted index maps each unique term to a list of document IDs (postings) where that term appears.

This structure is what allows search engines to perform lightning-fast lookups. Instead of scanning every document for a keyword, the engine simply jumps to the term in the index and retrieves its associated list of documents.
Why use SPIMI (Single Pass In-Memory Indexing)?

Building an index for millions of documents often exceeds the available RAM. SPIMI is a highly efficient indexing algorithm that solves this by:

    Memory Efficiency: It processes documents one by one, adding terms to an in-memory dictionary.

    No Sorting Required: Unlike traditional "sort-based" indexing, SPIMI collects postings directly into lists. When memory is full, it sorts the dictionary and writes that "block" to the disk.

    Scalability: Once all blocks are written, they are merged into one final index. This "single pass" approach avoids the overhead of maintaining a massive global term-to-ID mapping in memory.

The Necessity of Doc-ID Compression

In a large-scale index, the "postings lists" (the lists of document IDs) consume the most space. We compress these IDs for two primary reasons:

    Space Efficiency: Storing raw 32-bit or 64-bit integers for every occurrence of a word is incredibly wasteful.

    Increased Speed (I/O): Modern CPUs are much faster than Disk I/O. By compressing the data, we reduce the amount of data that needs to be read from the disk. Decompressing the data in RAM is significantly faster than reading uncompressed, bulky files from the drive.

How we compress: Delta Encoding (d-gaps)

Instead of storing absolute Document IDs (e.g., [100, 105, 110]), we store the gaps between them (e.g., [100, 5, 5]). Since these gaps are much smaller numbers, they can be represented using fewer bits through algorithms like Variable Byte Encoding or Simple-16 Encoding.

Supported Algorithms

keSE allows users to toggle between different strategies for index compression and document retrieval depending on the performance requirements (e.g., speed vs. storage space).

Index Compression Algorithms

Algorithm,Type,Description
VarByte,Byte-aligned,"Extremely fast decoding; uses a ""continuation bit"" to signal the end of a number."
Simple-9,Bit-packing,Packs multiple small integers into a single 32-bit word using 9 fixed-bit patterns.
Simple-16,Bit-packing,An optimized version of Simple-9 that uses 16 patterns to utilize 32-bit words more efficiently.
PforDelta,Frame-of-Reference,"Compresses most values in a ""frame"" using a small number of bits, while handling outliers as ""exceptions."""
Rice Coding,Entropy-based,Uses a quotient and remainder approach; highly effective for data following a geometric distribution.

Retrieval & Ranking Algorithms

keSE implements dynamic pruning techniques while serving queries. These allow the engine to find the top-k documents without evaluating every single document in the postings list.

Traditional Retrieval

    Boolean Retrieval: The baseline model for exact matches using AND, OR, and NOT logic.

Early Termination & Dynamic Pruning

These algorithms significantly speed up queries by "skipping" documents that cannot mathematically enter the top-k results:

    WAND (Weak AND): Uses an upper-bound score to skip documents that don't meet a specific threshold.

    Max Score: Partitions the query terms into "essential" and "optional" groups based on their maximum possible contribution to the final score.

    Block Max WAND: An optimized WAND that uses block-level metadata to skip entire chunks of the index at once.

    Block Max Max Score: Combines the strategy of Max Score with block-level score bounds for even tighter pruning.

DATASET

Our current inverted index is built on a processed wikipedia dump at https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2. 
The data is cleaned usng a python script which compressed it in batches into zstd files.
The compressed size of the wikipedia dump is 24.8 GB.

However the search engine can work on any type of data as long as it is cleaned.

HOW TO USE 

A cli is used to interact with the search engine. This cli needs to know a certain amount of information which we present to it in the form of a config.json. This config.json file should be placed in the base folder(the same folder with the README and Cargo.toml files).
```json
{
  "index_dir": "The resultant directory into which your index is going to be built", 
  "dataset_dir": "The directory which contains the dataset on which your index is going to be built",
  "compression_algo": "The compression algorithm you want your search engine to use",
  "query_algo": "The query algorithm you want your search engine to use"
}
```

COMMANDS

index-> Starts the SPIMI process and builds your index into the directory you have provided. The resultant index file is inverted_index.idx.
save-> The index needs some metadata about the index to serve queries. This commands saves this metadata in the index directory so that you can reuse
your inverted_index when you start the cli again.
load-> Loads your saved metadata so that you can start querying the index again.
metadata-> Produces metadata about your index like the size of the index, the number of terms and documents in the index etc.
query [QUERY STRING]-> Queries your inverted index for the [QUERY STRING] and returns the most relevant documents.
quit-> Exits the cli. If you havent saved the index metadata, you will have to rebuild the index from scratch next time.

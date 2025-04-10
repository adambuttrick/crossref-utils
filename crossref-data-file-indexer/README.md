# Crossref Data File Indexer

Rust utility for scanning an input directory containing the Crossref data file JSONL.gz files and creating three CSV indices describing their contents:

1. `member_index.csv`: Maps member IDs to input files
2. `prefix_index.csv`: Maps member IDs and DOI prefixes to input files
3. `doi_index.csv`: Maps DOIs, member IDs, and prefixes to input files

## Installation

```bash
cargo install crossref-data-file-indexer
```

## Usage

```bash
crossref-data-file-indexer --input-dir /path/to/crossref/files --output-dir /path/to/output

# With additional options
crossref-data-file-indexer --input-dir /path/to/crossref/files --output-dir /path/to/output --threads 8 --log-level DEBUG --stats-interval 30
```

## Options

- `--input-dir, -i`: Directory containing the input files (required)
- `--output-dir, -o`: Directory where output CSV index files will be saved (required)
- `--threads, -t`: Number of threads to use (default: auto-detect)
- `--log-level, -l`: Logging level (TRACE, DEBUG, INFO, WARN, ERROR) (default: INFO)
- `--stats-interval, -s`: Interval in seconds to log statistics (default: 60)

# Crossref Data File Organizer

Rust utility for reorganizing Crossref data files (JSONL.gz) by member ID and DOI prefix.

## Installation

```bash
cargo install crossref-data-file-organizer
```

## Usage

```bash
crossref-data-file-organizer --input /path/to/input/files --output-dir /path/to/output
```

### Command Line Options

```
USAGE:
    crossref-data-file-organizer [OPTIONS] --input <INPUT> --output-dir <OUTPUT_DIR>

OPTIONS:
    -i, --input <INPUT>                           Directory containing input JSONL.gz files
    -o, --output-dir <OUTPUT_DIR>                 Base directory for organized output structure
        --temp-dir <TEMP_DIR>                     Temporary directory for intermediate files
        --max-intermediate-files <NUM>            Max open intermediate member files (Pass 1) [default: 256]
        --max-final-files-per-member <NUM>        Max open final prefix files per member (Pass 2) [default: 128]
    -l, --log-level <LEVEL>                       Logging level (DEBUG, INFO, WARN, ERROR) [default: INFO]
    -t, --threads <NUM>                           Number of threads to use (0 for auto) [default: 0]
        --filter-file <FILE>                      Path to a JSON file specifying filters
    -s, --stats-interval <SECONDS>                Interval in seconds to log statistics [default: 60]
    -h, --help                                    Print help information
    -V, --version                                 Print version information
```

## Design
This utitlity uses a two passes to process the data file in a quick and memory efficient fashion:

### Pass 1: Distribution

In the first pass, we process the input files in parallel using Rayon to distribute work across multiple cores, streaming and decompressing on-the-fly, while avoiding loading the entire files into memory. When using the filter options, we employ byte pattern matching in the JSON parsing, allowing us to quickly skip irrelevant records.  To manage system resources, an LRU cache limits open file handles for intermediate files.

### Pass 2: Consolidation

In the second pass processes each member's intermediate files concurrently, but with each member's processing being done independently to minimize lock contention. Records are streamed out line-by-line without (vs. loading the entire files), then organized by DOI prefix and compressed on-the-fly using flate2.


## Output Structure

```
output_dir/
├── member_id_1/
│   ├── prefix_1/
│   │   └── data.jsonl.gz
│   └── prefix_2/
│       └── data.jsonl.gz
├── member_id_2/
│   └── prefix_3/
│       └── data.jsonl.gz
└── ...
```

## Filtering

Create a JSON file with the following structure to filter by member ID and optionally by DOI prefix:

```json
{
  "member_id_1": 1,              // Include all prefixes for this member
  "member_id_2": ["10.1234", "10.5678"]  // Include only specific prefixes
}
```

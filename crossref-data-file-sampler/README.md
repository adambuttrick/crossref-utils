# Crossref Data File Sampler

Rust utility for randomly sampling JSON records from Crossref data files. Efficiently samples lines from compressed JSONL files (`.jsonl.gz`) from the Crossref data file using reservoir sampling to ensure a statistically representative random sample.


## Installation

```bash
cargo build --release
```

## Usage

```bash
crossref-data-file-sampler -i <INPUT_DIR> -o <OUTPUT_FILE> -n <SAMPLE_COUNT> [OPTIONS]
```

### Required Arguments

- `-i, --input-dir <DIR>`: Directory containing input JSONL.gz files (will recursively search)
- `-o, --output-file <FILE>`: Path where sampled lines will be written
- `-n, --sample-count <NUM>`: Number of lines to sample

### Optional Arguments

- `-l, --log-level <LEVEL>`: Logging level (TRACE, DEBUG, INFO, WARN, ERROR) [default: INFO]
- `-t, --threads <NUM>`: Number of threads to use (0 for auto-detection) [default: 0]
- `--no-shuffle`: Don't shuffle the final sample (faster, but output order depends on processing order)
- `-h, --help`: Show help information
- `-V, --version`: Show version information

## Example

```bash
# Sample 10,000 records from all JSONL.gz files in the data directory
crossref-data-file-sampler -i ./crossref_data -o ./sample_output.jsonl -n 10000

# Use 4 threads and disable final shuffling
crossref-data-file-sampler -i ./crossref_data -o ./sample_output.jsonl -n 10000 -t 4 --no-shuffle
```

## Notes

- For very large datasets, sampling without final shuffling (`--no-shuffle`) uses less memory

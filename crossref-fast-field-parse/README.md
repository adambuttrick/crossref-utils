# crossref-fast-field-parse

Rust utility for extracting specific fields from Crossref data files in JSONL.gz format.

## Usage

```
crossref-fast-field-parse --input <DIRECTORY> --fields <FIELD_LIST> [OPTIONS]
```

### Required Arguments

- `-i, --input <DIRECTORY>`: Directory containing JSONL.gz files to process
- `-f, --fields <FIELD_LIST>`: Comma-separated list of fields to extract (e.g., 'author.family,title,ISSN')

### Options

- `-o, --output <FILE_OR_DIR>`: Output CSV file or directory (default: "field_data.csv")
- `-l, --log-level <LEVEL>`: Logging level (DEBUG, INFO, WARN, ERROR) (default: "INFO")
- `-t, --threads <NUM>`: Number of threads to use (0 for auto) (default: 0)
- `-b, --batch-size <SIZE>`: Target number of records per batch sent to writer (default: 10000)
- `-s, --stats-interval <SECONDS>`: Interval in seconds to log statistics (default: 60)
- `-g, --organize`: Organize output by member ID
- `--member <ID>`: Filter by member ID
- `--doi-prefix <PREFIX>`: Filter by DOI prefix
- `--max-open-files <NUM>`: Maximum number of open files when using --organize (default: 100)

## Examples

### Basic Usage

Extract 'title' and 'author.affiliation.name' fields from all files in a directory:

```
crossref-fast-field-parse --input ./crossref_data --fields "title,author.affiliation.name" --output results.csv
```

### Extracting Nested Fields

Extract author names and affiliations:

```
crossref-fast-field-parse --input ./crossref_data --fields "author.family,author.given,author.affiliation.name" --output author_data.csv
```

### Organize Output by Member

Create separate CSV files for each member:

```
crossref-fast-field-parse --input ./crossref_data --fields "title,DOI,publisher" --organize --output ./members_output
```

### Filter by Member or DOI Prefix

Process only records from a specific member:

```
crossref-fast-field-parse --input ./crossref_data --fields "title,DOI" --member 78 --output filtered_results.csv
```

### Performance Tuning

Specify thread count and batch size for optimal performance:

```
crossref-fast-field-parse --input ./crossref_data --fields "title,DOI" --threads 8 --batch-size 50000 --output results.csv
```

## Output Format

The tool generates CSV files with the following columns:

- `doi`: DOI of the record
- `field_name`: Name of the extracted field
- `subfield_path`: Complete path to the subfield (includes array indices)
- `value`: Extracted value
- `member_id`: Crossref member ID
- `doi_prefix`: DOI prefix

## Performance Considerations

- Processing speed depends on the number of fields being extracted and their complexity
- Use more threads on multi-core systems for better performance
- Adjust batch size based on available memory
- Monitor memory usage with the periodic statistics output

## Notes

This tool uses the `flate2` crate for decompression. By default, it's configured to use`zlib-ng` backend for faster processing of compressed files. If you don't have or don't want to install `zlib-ng`, modify the dependency in `Cargo.toml`:

```toml
# Use standard flate2 crate if you don't have (or want to install) zlib-ng 
flate2 = "1.1.1"

# For faster decompression with zlib-ng
# flate2 = { version = "1.1.1", features = ["zlib-ng"], default-features = false }
```

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use rayon::prelude::*;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

const INPUT_FILE_COL: &str = "input_file";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, value_name = "DIR")]
    input_dir: PathBuf,

    #[arg(short, long, value_name = "DIR")]
    output_dir: PathBuf,

    #[arg(short, long, value_name = "PATH")]
    new_path: Option<PathBuf>,

    #[arg(short, long, action)]
    strip_only: bool,
}

fn process_csv_file(
    input_path: &Path,
    output_path: &Path,
    new_path_base: Option<&Path>,
    strip_only: bool,
) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "Failed to create output directory '{}'",
                parent.display()
            )
        })?;
    }

    let input_file = File::open(input_path)
        .with_context(|| format!("Failed to open input file '{}'", input_path.display()))?;
    let output_file = File::create(output_path)
        .with_context(|| format!("Failed to create output file '{}'", output_path.display()))?;

    let reader = std::io::BufReader::new(input_file);
    let writer = std::io::BufWriter::new(output_file);

    let mut rdr = ReaderBuilder::new().from_reader(reader);
    let mut wtr = WriterBuilder::new().from_writer(writer);

    let headers = rdr.headers()?.clone();
    let input_file_col_idx = headers.iter().position(|h| h == INPUT_FILE_COL);

    wtr.write_record(&headers)?;

    let mut record = StringRecord::new();
    while rdr.read_record(&mut record)? {
        if let Some(idx) = input_file_col_idx {
            if let Some(original_path_str) = record.get(idx) {
                if !original_path_str.trim().is_empty() {
                    let original_path = Path::new(original_path_str);
                    if let Some(filename_osstr) = original_path.file_name() {
                        let filename = filename_osstr.to_str().ok_or_else(|| {
                            anyhow!(
                                "Filename is not valid UTF-8 in record: {:?}",
                                record
                            )
                        })?;

                        let new_value: String = if strip_only {
                            filename.to_string()
                        } else if let Some(np) = new_path_base {
                            np.join(filename)
                                .to_str()
                                .ok_or_else(|| {
                                    anyhow!(
                                        "Resulting path is not valid UTF-8 for filename: {}",
                                        filename
                                    )
                                })?
                                .to_string()
                        } else {
                            original_path_str.to_string()
                        };

                        if new_value != original_path_str {
                            let mut modified_record = StringRecord::with_capacity(record.len(), record.as_slice().len()); // Pre-allocate approx size
                            for (i, field) in record.iter().enumerate() {
                                if i == idx {
                                    modified_record.push_field(&new_value);
                                } else {
                                    modified_record.push_field(field);
                                }
                            }
                            wtr.write_record(&modified_record)?;
                        } else {
                            wtr.write_record(&record)?;
                        }
                        continue;
                    }
                }
            }
        }
        wtr.write_record(&record)?;
    }

    wtr.flush()?;
    println!("Processed {} -> {}", input_path.display(), output_path.display());
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    if !args.strip_only && args.new_path.is_none() {
        return Err(anyhow!(
            "--new-path <PATH> is required unless --strip-only is used"
        ));
    }

    let pattern = args
        .input_dir
        .join("*.csv")
        .to_str()
        .ok_or_else(|| anyhow!("Input directory path is not valid UTF-8"))?
        .to_string();

    let input_files: Vec<Result<PathBuf, glob::GlobError>> = glob::glob(&pattern)?.collect();
    let mut successful_paths = Vec::new();
    let mut glob_errors = Vec::new();
    for entry in input_files {
        match entry {
            Ok(path) => successful_paths.push(path),
            Err(e) => glob_errors.push(e),
        }
    }

    if !glob_errors.is_empty() {
         eprintln!("Warning: Encountered errors during file search:");
         for error in glob_errors {
             eprintln!(" - {}", error);
         }
    }

    if successful_paths.is_empty() {
        println!("No CSV files found matching pattern '{}'", pattern);
        return Ok(());
    }


    println!(
        "Found {} CSV files. Processing...",
        successful_paths.len()
    );

    let results: Vec<Result<()>> = successful_paths
        .par_iter()
        .map(|input_path| {
            let filename = input_path
                .file_name()
                .ok_or_else(|| anyhow!("Failed to get filename for {}", input_path.display()))?;

            let output_path = args.output_dir.join(filename);

            process_csv_file(
                input_path,
                &output_path,
                args.new_path.as_deref(),
                args.strip_only,
            )
            .with_context(|| format!("Error processing file: {}", input_path.display()))
        })
        .collect();

    let mut errors = 0;
    for result in results {
        if let Err(e) = result {
            eprintln!("{:?}", e);
            errors += 1;
        }
    }

    if errors > 0 {
        eprintln!(
            "\nProcessing finished with {} error(s).",
            errors
        );
        return Err(anyhow!("{} files failed to process", errors));
    }

    println!(
        "\nAll {} files processed successfully. Output saved to {}",
        successful_paths.len(), args.output_dir.display()
    );
    Ok(())
}
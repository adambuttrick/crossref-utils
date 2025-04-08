use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write, copy},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use simple_logger::SimpleLogger;
use time::macros::format_description;

mod memory_usage {
    use log::info;

    #[derive(Debug)]
    pub struct MemoryStats {
        pub rss_mb: f64,
        pub vm_size_mb: f64,
        pub percent: Option<f64>,
    }

    #[cfg(target_os = "linux")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::fs::read_to_string;

        let pid = std::process::id();
        let status_file = format!("/proc/{}/status", pid);
        let content = read_to_string(status_file).ok()?;

        let mut vm_rss_kb = None;
        let mut vm_size_kb = None;

        for line in content.lines() {
            if line.starts_with("VmRSS:") {
                vm_rss_kb = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse::<f64>().ok());
            } else if line.starts_with("VmSize:") {
                vm_size_kb = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse::<f64>().ok());
            }
            if vm_rss_kb.is_some() && vm_size_kb.is_some() {
                break;
            }
        }

        let rss_mb = vm_rss_kb? / 1024.0;
        let vm_size_mb = vm_size_kb? / 1024.0;
        let mut percent = None;

        if let Ok(meminfo) = read_to_string("/proc/meminfo") {
            if let Some(mem_total_kb) = meminfo
                .lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|s| s.parse::<f64>().ok())
            {
                if mem_total_kb > 0.0 {
                    percent = Some((vm_rss_kb.unwrap_or(0.0) / mem_total_kb) * 100.0);
                }
            }
        }

        Some(MemoryStats {
            rss_mb,
            vm_size_mb,
            percent,
        })
    }

    #[cfg(target_os = "macos")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command;

        let pid = std::process::id();
        let ps_output = Command::new("ps")
            .args(&["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let rss_kb = String::from_utf8_lossy(&ps_output.stdout)
            .trim()
            .parse::<f64>()
            .ok()?;

        let vsz_output = Command::new("ps")
            .args(&["-o", "vsz=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let vsz_kb = String::from_utf8_lossy(&vsz_output.stdout)
            .trim()
            .parse::<f64>()
            .ok()?;

        let rss_mb = rss_kb / 1024.0;
        let vm_size_mb = vsz_kb / 1024.0;
        let mut percent = None;

        if let Ok(hw_mem_output) = Command::new("sysctl").args(&["-n", "hw.memsize"]).output() {
            if let Ok(total_bytes_str) = String::from_utf8(hw_mem_output.stdout) {
                if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() {
                    let total_kb = total_bytes / 1024.0;
                    if total_kb > 0.0 {
                        percent = Some((rss_kb / total_kb) * 100.0);
                    }
                }
            }
        }

        Some(MemoryStats {
            rss_mb,
            vm_size_mb,
            percent,
        })
    }

    #[cfg(target_os = "windows")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command;

        let pid = std::process::id();
        let tasklist_output = Command::new("tasklist")
            .args(&["/fi", &format!("PID eq {}", pid), "/fo", "csv", "/nh"])
            .output()
            .ok()?;

        let output_str = String::from_utf8_lossy(&tasklist_output.stdout);
        let fields: Vec<&str> = output_str.trim().split(',').collect();
        if fields.len() < 2 { return None; }

        let mem_usage_str = fields.last()?.trim().trim_matches('"');

        let rss_kb = mem_usage_str
            .trim_end_matches(" K")
            .replace(',', "")
            .parse::<f64>()
            .ok()?;

        let vm_size_mb = 0.0;
        let rss_mb = rss_kb / 1024.0;
        let mut percent = None;

        if let Ok(mem_output) = Command::new("wmic")
            .args(&["ComputerSystem", "get", "TotalPhysicalMemory", "/value"])
            .output()
        {
            let mem_str = String::from_utf8_lossy(&mem_output.stdout);
            if let Some(total_bytes_str) = mem_str
                .lines()
                .find(|line| line.starts_with("TotalPhysicalMemory="))
                .and_then(|line| line.split('=').nth(1))
            {
                if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() {
                    let total_kb = total_bytes / 1024.0;
                    if total_kb > 0.0 {
                        percent = Some((rss_kb / total_kb) * 100.0);
                    }
                }
            }
        }

        Some(MemoryStats {
            rss_mb,
            vm_size_mb,
            percent,
        })
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        None
    }

    pub fn log_memory_usage(note: &str) {
        if let Some(stats) = get_memory_usage() {
            let percent_str = stats
                .percent
                .map_or_else(|| "N/A".to_string(), |p| format!("{:.1}%", p));
            let vm_str = if stats.vm_size_mb > 0.0 {
                format!("{:.1} MB virtual", stats.vm_size_mb)
            } else {
                "".to_string()
            };
            let comma = if !vm_str.is_empty() { ", " } else { "" };
            info!(
                "Memory usage ({}): {:.1} MB physical (RSS){}{} {} of system memory",
                note, stats.rss_mb, comma, vm_str, percent_str
            );
        } else {
            info!(
                "Memory usage tracking not available or failed on this platform ({})",
                std::env::consts::OS
            );
        }
    }
}


#[derive(Parser)]
#[command(name = "Crossref Indexer")]
#[command(about = "Scans Crossref JSONL.gz files and creates CSV indexes for members, prefixes, and DOIs.")]
#[command(version = "1.1.0")]
struct Cli {
    #[arg(
        short,
        long,
        help = "Directory containing input JSONL.gz files",
        required = true
    )]
    input_dir: String,

    #[arg(
        short,
        long,
        help = "Directory where output CSV index files will be saved",
        required = true
    )]
    output_dir: String,

    #[arg(
        short,
        long,
        default_value = "INFO",
        help = "Logging level (TRACE, DEBUG, INFO, WARN, ERROR)"
    )]
    log_level: String,

    #[arg(
        short,
        long,
        default_value = "0",
        help = "Number of threads to use (0 for auto)"
    )]
    threads: usize,

    #[arg(
        short = 's',
        long,
        default_value = "60",
        help = "Interval in seconds to log statistics"
    )]
    stats_interval: u64,
}

type MemberId = String;
type DoiPrefix = String;
type Doi = String;
type FilePath = String;

type MemberIndexSet = HashSet<(MemberId, FilePath)>;
type PrefixIndexSet = HashSet<(MemberId, DoiPrefix, FilePath)>;
type DoiIndexSet = HashSet<(Doi, MemberId, DoiPrefix, FilePath)>;

#[derive(Deserialize, Debug)]
struct IndexRecord {
    member: Option<Value>,
    prefix: Option<String>,
    #[serde(rename = "DOI")]
    doi: Option<String>,
}

#[derive(Debug, Clone)]
struct FileStats {
    lines_read: u64,
    json_parse_errors: u64,
    member_id_missing: u64,
    prefix_missing: u64,
    doi_missing: u64,
}

impl Default for FileStats {
    fn default() -> Self {
        FileStats {
            lines_read: 0,
            json_parse_errors: 0,
            member_id_missing: 0,
            prefix_missing: 0,
            doi_missing: 0,
        }
    }
}

#[derive(Default)]
struct AggregateStats {
    total_lines_read: AtomicU64,
    total_json_parse_errors: AtomicU64,
    total_member_id_missing: AtomicU64,
    total_prefix_missing: AtomicU64,
    total_doi_missing: AtomicU64,
    final_unique_members: AtomicU64,
    final_unique_prefixes: AtomicU64,
    final_unique_dois: AtomicU64,
}

impl AggregateStats {
    fn new() -> Self {
        Default::default()
    }

    fn update_from_final(&self, final_stats: &FileStats, counts: (u64, u64, u64)) {
        self.total_lines_read
            .store(final_stats.lines_read, Ordering::Relaxed);
        self.total_json_parse_errors
            .store(final_stats.json_parse_errors, Ordering::Relaxed);
        self.total_member_id_missing
            .store(final_stats.member_id_missing, Ordering::Relaxed);
        self.total_prefix_missing
            .store(final_stats.prefix_missing, Ordering::Relaxed);
        self.total_doi_missing
            .store(final_stats.doi_missing, Ordering::Relaxed);
        self.final_unique_members
            .store(counts.0, Ordering::Relaxed);
        self.final_unique_prefixes
            .store(counts.1, Ordering::Relaxed);
        self.final_unique_dois
            .store(counts.2, Ordering::Relaxed);
    }

    fn log_current_stats(&self, stage: &str) {
        info!("--- Periodic Stats ({}) ---", stage);
        info!(
            " Files Processed (Estimate): {} / ?",
            self.total_lines_read.load(Ordering::Relaxed) / 50000
        );
        info!(
            " Lines Read (so far): {}",
            self.total_lines_read.load(Ordering::Relaxed)
        );
        info!(
            " JSON Parse Errors: {}",
            self.total_json_parse_errors.load(Ordering::Relaxed)
        );
        info!(
            " Records Missing Member ID: {}",
            self.total_member_id_missing.load(Ordering::Relaxed)
        );
        info!(
            " Records Missing Prefix: {}",
            self.total_prefix_missing.load(Ordering::Relaxed)
        );
        info!(
            " Records Missing DOI: {}",
            self.total_doi_missing.load(Ordering::Relaxed)
        );
        info!("------------------------------");
    }

    fn log_final_stats(&self, processing_complete: bool) {
        info!("--- Final Stats Summary ---");
        info!(
            " Total Lines Read: {}",
            self.total_lines_read.load(Ordering::Relaxed)
        );
        info!(
            " Total JSON Parse Errors: {}",
            self.total_json_parse_errors.load(Ordering::Relaxed)
        );
        info!(
            " Total Records Missing Member ID: {}",
            self.total_member_id_missing.load(Ordering::Relaxed)
        );
        info!(
            " Total Records Missing Prefix: {}",
            self.total_prefix_missing.load(Ordering::Relaxed)
        );
        info!(
            " Total Records Missing DOI: {}",
            self.total_doi_missing.load(Ordering::Relaxed)
        );
        if processing_complete {
             info!(
                " Unique (Member, File) Pairs Found: {}",
                self.final_unique_members.load(Ordering::Relaxed)
            );
            info!(
                " Unique (Member, Prefix, File) Pairs Found: {}",
                self.final_unique_prefixes.load(Ordering::Relaxed)
            );
             info!(
                " Unique (DOI, Member, Prefix, File) Records Found: {}",
                self.final_unique_dois.load(Ordering::Relaxed)
            );
        } else {
             info!(" Final unique counts not available due to processing errors.");
        }

        info!("---------------------------");
    }
}

fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.jsonl.gz");
    let pattern_str = pattern.to_string_lossy();
    info!("Searching for files matching pattern: {}", pattern_str);
    let paths: Vec<PathBuf> = glob(&pattern_str)?
        .filter_map(Result::ok)
        .collect();
    if paths.is_empty() {
        warn!("No files found matching the pattern: {}", pattern_str);
    }
    Ok(paths)
}

fn extract_index_fields(record: &IndexRecord) -> Option<(MemberId, DoiPrefix, Doi)> {
    let member_id = record.member.as_ref().and_then(|v| {
        if v.is_string() {
            v.as_str().map(|s| s.to_string())
        } else if v.is_number() {
            Some(v.to_string())
        } else {
            None
        }
    })?;

    let doi = record.doi.as_ref().cloned()?;

    let prefix = record
        .prefix
        .as_ref()
        .cloned()
        .or_else(|| doi.split_once('/').map(|(pfx, _)| pfx.to_string()))
        .unwrap_or_else(|| "_unknown_".to_string());

    Some((member_id, prefix, doi))
}

fn extract_member_id_from_value(member_value: &Option<Value>) -> Option<MemberId> {
     member_value.as_ref().and_then(|v| {
         if v.is_string() {
             v.as_str().map(|s| s.to_string())
         } else if v.is_number() {
             Some(v.to_string())
         } else {
             None
         }
     })
}


fn format_elapsed(elapsed: Duration) -> String {
    let total_secs = elapsed.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let millis = elapsed.subsec_millis();

    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}.{:03}s", seconds, millis)
    }
}

fn write_partial_index_csv<T, P, F>(
    data: &HashSet<T>,
    output_path: P,
    record_to_csv: F,
) -> Result<()>
where
    T: Eq + std::hash::Hash,
    P: AsRef<Path>,
    F: Fn(&T, &mut csv::Writer<BufWriter<File>>) -> Result<()>,
{
    let file = File::create(output_path.as_ref())
        .with_context(|| format!("Failed to create partial CSV file: {}", output_path.as_ref().display()))?;
    let buf_writer = BufWriter::new(file);
    let mut wtr = csv::Writer::from_writer(buf_writer);

    for item in data {
        record_to_csv(item, &mut wtr)
            .with_context(|| format!("Failed to write record to partial CSV: {}", output_path.as_ref().display()))?;
    }

    wtr.flush().context("Failed to flush partial CSV writer")?;
    Ok(())
}

fn process_file_for_index(
    filepath: &PathBuf,
    output_dir: &PathBuf,
    pb_clone: &ProgressBar,
    global_stats: Arc<AggregateStats>,
) -> Result<FileStats, String> {

    let file_path_str = filepath.to_string_lossy().to_string();
    let file_stem = filepath.file_stem().unwrap_or_default().to_string_lossy();

    pb_clone.set_message(format!("Processing: {}", file_path_str));
    debug!("Starting processing for {}", filepath.display());

    let mut local_member_index: MemberIndexSet = HashSet::new();
    let mut local_prefix_index: PrefixIndexSet = HashSet::new();
    let mut local_doi_index: DoiIndexSet = HashSet::new();
    let mut local_stats = FileStats::default();

    let file = match File::open(filepath) {
        Ok(f) => f,
        Err(e) => return Err(format!("Failed to open {}: {}", filepath.display(), e)),
    };

    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    for (line_num, line_result) in reader.lines().enumerate() {
        local_stats.lines_read += 1;

        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                warn!(
                    "Error reading line {} from {}: {}",
                    line_num + 1,
                    filepath.display(),
                    e
                );
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<IndexRecord>(&line) {
            Ok(record) => {
                match extract_index_fields(&record) {
                     Some((member_id, prefix, doi)) => {
                         local_member_index.insert((member_id.clone(), file_path_str.clone()));
                         local_prefix_index.insert((member_id.clone(), prefix.clone(), file_path_str.clone()));
                         local_doi_index.insert((doi, member_id, prefix, file_path_str.clone()));
                     }
                     None => {
                         if extract_member_id_from_value(&record.member).is_none() {
                             local_stats.member_id_missing += 1;
                         }
                          if record.doi.is_none() {
                             local_stats.doi_missing += 1;
                             if record.prefix.is_none() {
                                local_stats.prefix_missing += 1;
                             }
                         } else {
                             if record.prefix.is_none() && !record.doi.as_ref().unwrap().contains('/') {
                                 local_stats.prefix_missing += 1;
                             }
                         }
                     }
                 }
            }
            Err(e) => {
                local_stats.json_parse_errors += 1;
                 if local_stats.json_parse_errors < 5 || local_stats.json_parse_errors % 10000 == 0 {
                     warn!(
                         "JSON parse error in {} line ~{}: {} (Line starts: {}...)",
                         filepath.display(),
                         line_num + 1,
                         e,
                         line.chars().take(100).collect::<String>()
                     );
                 }
            }
        }

         if local_stats.lines_read % 50000 == 0 {
              global_stats.total_lines_read.fetch_add(50000, Ordering::Relaxed);
              global_stats.total_json_parse_errors.fetch_add(local_stats.json_parse_errors, Ordering::Relaxed);
              global_stats.total_member_id_missing.fetch_add(local_stats.member_id_missing, Ordering::Relaxed);
              global_stats.total_prefix_missing.fetch_add(local_stats.prefix_missing, Ordering::Relaxed);
              global_stats.total_doi_missing.fetch_add(local_stats.doi_missing, Ordering::Relaxed);
              local_stats.json_parse_errors = 0;
              local_stats.member_id_missing = 0;
              local_stats.prefix_missing = 0;
              local_stats.doi_missing = 0;
              pb_clone.set_message(format!("Processing: {} (line ~{})", file_path_str, local_stats.lines_read));
         }
    }

    debug!("Finished reading {}, writing partial index files...", filepath.display());
    let write_start = Instant::now();

    let member_part_path = output_dir.join(format!("{}.member.part", file_stem));
    let prefix_part_path = output_dir.join(format!("{}.prefix.part", file_stem));
    let doi_part_path = output_dir.join(format!("{}.doi.part", file_stem));

    if !local_member_index.is_empty() {
        if let Err(e) = write_partial_index_csv(&local_member_index, &member_part_path, |(member_id, f_path), wtr| {
            wtr.write_record(&[member_id, f_path])?;
            Ok(())
        }) {
            return Err(format!("Failed to write partial member index for {}: {}", filepath.display(), e));
        }
        debug!("Wrote {} member entries to {}", local_member_index.len(), member_part_path.display());
    }

    if !local_prefix_index.is_empty() {
         if let Err(e) = write_partial_index_csv(&local_prefix_index, &prefix_part_path, |(member_id, prefix, f_path), wtr| {
             wtr.write_record(&[member_id, prefix, f_path])?;
             Ok(())
         }) {
             return Err(format!("Failed to write partial prefix index for {}: {}", filepath.display(), e));
         }
         debug!("Wrote {} prefix entries to {}", local_prefix_index.len(), prefix_part_path.display());
    }

     if !local_doi_index.is_empty() {
         if let Err(e) = write_partial_index_csv(&local_doi_index, &doi_part_path, |(doi, member_id, prefix, f_path), wtr| {
             wtr.write_record(&[doi, member_id, prefix, f_path])?;
             Ok(())
         }) {
             return Err(format!("Failed to write partial DOI index for {}: {}", filepath.display(), e));
         }
          debug!("Wrote {} DOI entries to {}", local_doi_index.len(), doi_part_path.display());
    }

    debug!("Partial file writing for {} took {}", filepath.display(), format_elapsed(write_start.elapsed()));
     let remaining_lines = local_stats.lines_read % 50000;
     if remaining_lines > 0 {
          global_stats.total_lines_read.fetch_add(remaining_lines, Ordering::Relaxed);
     }
      global_stats.total_json_parse_errors.fetch_add(local_stats.json_parse_errors, Ordering::Relaxed);
      global_stats.total_member_id_missing.fetch_add(local_stats.member_id_missing, Ordering::Relaxed);
      global_stats.total_prefix_missing.fetch_add(local_stats.prefix_missing, Ordering::Relaxed);
      global_stats.total_doi_missing.fetch_add(local_stats.doi_missing, Ordering::Relaxed);

    pb_clone.inc(1);
    Ok(FileStats {
        lines_read: local_stats.lines_read,
        json_parse_errors: local_stats.json_parse_errors,
        member_id_missing: local_stats.member_id_missing,
        prefix_missing: local_stats.prefix_missing,
        doi_missing: local_stats.doi_missing,
    })
}

fn concatenate_partial_files(
    output_dir: &Path,
    part_extension: &str,
    final_output_path: &Path,
    headers: &[&str],
) -> Result<u64> {
    info!("Concatenating *{} files into {}", part_extension, final_output_path.display());
    let start_time = Instant::now();

    let pattern = output_dir.join(format!("*{}", part_extension));
    let pattern_str = pattern.to_string_lossy();

    let part_files: Vec<PathBuf> = glob(&pattern_str)?
        .filter_map(Result::ok)
        .collect();

    if part_files.is_empty() {
        warn!("No partial files found matching pattern: {}", pattern_str);
        let mut wtr = csv::Writer::from_path(final_output_path)?;
        wtr.write_record(headers)?;
        wtr.flush()?;
        return Ok(0);
    }

    info!("Found {} partial files to concatenate.", part_files.len());

    let final_file = File::create(final_output_path)
        .with_context(|| format!("Failed to create final output file: {}", final_output_path.display()))?;
    let mut final_writer = BufWriter::new(final_file);

    {
        let mut csv_wtr = csv::Writer::from_writer(&mut final_writer);
        csv_wtr.write_record(headers)
            .context("Failed to write header to final CSV")?;
        csv_wtr.flush().context("Failed to flush header for final CSV")?;
    }

    let mut total_records: u64 = 0;
    let mut errors: Vec<String> = Vec::new();
    let mut files_to_delete: Vec<PathBuf> = Vec::new();

    let pb = ProgressBar::new(part_files.len() as u64);
     pb.set_style(
         ProgressStyle::default_bar()
             .template("[{elapsed_precise}] Concatenating {msg}: [{bar:40.green/blue}] {pos}/{len} ")
             .expect("Failed to create progress bar template")
             .progress_chars("=> "),
     );
     pb.set_message(part_extension.to_string());


    for part_file_path in part_files {
        pb.inc(1);
        debug!("Appending {}...", part_file_path.display());
        match File::open(&part_file_path) {
            Ok(part_file) => {
                let mut reader = BufReader::new(part_file);
                match copy(&mut reader, &mut final_writer) {
                    Ok(bytes_copied) => {
                          match count_lines(&part_file_path) {
                             Ok(count) => total_records += count,
                             Err(e) => errors.push(format!("Failed to count lines in {}: {}", part_file_path.display(), e)),
                         }
                         files_to_delete.push(part_file_path.clone());
                         debug!("Appended {} bytes from {}", bytes_copied, part_file_path.display());
                    }
                    Err(e) => {
                        errors.push(format!(
                            "Failed to copy data from {}: {}",
                            part_file_path.display(), e
                        ));
                    }
                }
            }
            Err(e) => {
                 errors.push(format!("Failed to open partial file {}: {}", part_file_path.display(), e));
            }
        }
    }

    final_writer.flush().context("Failed to flush final CSV writer")?;
    pb.finish_with_message(format!("Finished concatenating {}", part_extension));

     info!("Deleting {} partial files...", files_to_delete.len());
     let delete_pb = ProgressBar::new(files_to_delete.len() as u64);
      delete_pb.set_style(
         ProgressStyle::default_bar()
             .template("[{elapsed_precise}] Deleting parts: [{bar:40.red/yellow}] {pos}/{len} ")
             .expect("Failed to create progress bar template")
             .progress_chars("=> "),
     );

     for file_to_delete in files_to_delete {
        delete_pb.inc(1);
         if let Err(e) = fs::remove_file(&file_to_delete) {
             errors.push(format!("Failed to delete partial file {}: {}", file_to_delete.display(), e));
         } else {
             debug!("Deleted {}", file_to_delete.display());
         }
     }
     delete_pb.finish_with_message("Finished deleting parts.");


    if !errors.is_empty() {
        error!("Errors occurred during concatenation/deletion for {}:", part_extension);
        for err in &errors {
            error!("  - {}", err);
        }
    }


    info!(
        "Finished concatenating {} files for {} in {}. Total records: {}",
        part_extension, final_output_path.display(), format_elapsed(start_time.elapsed()), total_records
    );

    Ok(total_records)
}

fn count_lines(path: &Path) -> Result<u64> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().count() as u64)
}


fn main() -> Result<()> {
    let main_start_time = Instant::now();
    let cli = Cli::parse();

    let log_level = match cli.log_level.to_uppercase().as_str() {
        "TRACE" => LevelFilter::Trace,
        "DEBUG" => LevelFilter::Debug,
        "INFO" => LevelFilter::Info,
        "WARN" | "WARNING" => LevelFilter::Warn,
        "ERROR" => LevelFilter::Error,
        _ => {
            eprintln!("Invalid log level '{}', defaulting to INFO.", cli.log_level);
            LevelFilter::Info
        }
    };
    SimpleLogger::new()
        .with_level(log_level)
        .with_timestamp_format(format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second]"
        ))
        .init()?;

    info!("Starting Crossref Indexer v1.1.0");
    memory_usage::log_memory_usage("initial");

    let input_dir = PathBuf::from(&cli.input_dir);
    let output_dir = PathBuf::from(&cli.output_dir);

    fs::create_dir_all(&output_dir)
        .with_context(|| format!("Failed to create output directory: {}", output_dir.display()))?;

    let num_threads = if cli.threads == 0 {
        let cores = num_cpus::get();
        info!("Auto-detected {} CPU cores. Using {} threads.", cores, cores);
        cores
    } else {
        info!("Using specified {} threads.", cli.threads);
        cli.threads
    };
    if let Err(e) = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
    {
        error!(
            "Failed to build global thread pool with {} threads: {}. Proceeding with default.",
            num_threads, e
        );
    }

    info!("Input directory: {}", input_dir.display());
    info!("Output directory: {}", output_dir.display());
    info!("Statistics logging interval: {} seconds.", cli.stats_interval);

    let files = find_jsonl_gz_files(&input_dir)?;
    if files.is_empty() {
        warn!("No .jsonl.gz files found. Exiting.");
        return Ok(());
    }
    info!("Found {} input files to process.", files.len());

    let stats = Arc::new(AggregateStats::new());
    let stats_thread_running = Arc::new(Mutex::new(true));
    let stats_interval_duration = Duration::from_secs(cli.stats_interval);
    let stats_clone_for_thread = Arc::clone(&stats);
    let stats_thread_running_clone = Arc::clone(&stats_thread_running);

    let stats_thread = std::thread::spawn(move || {
        info!("Stats logging thread started.");
        let mut last_log_time = Instant::now();
        loop {
            if let Ok(guard) = stats_thread_running_clone.try_lock() {
                if !*guard {
                    info!("Stats thread received stop signal.");
                    break;
                }
            }

            std::thread::sleep(Duration::from_millis(500));

            if last_log_time.elapsed() >= stats_interval_duration {
                memory_usage::log_memory_usage("periodic check");
                stats_clone_for_thread.log_current_stats("Periodic");
                last_log_time = Instant::now();
            }
        }
        info!("Stats logging thread finished.");
    });


    info!("--- Starting Processing Pass (Reading, Parsing, Writing Partial Indices) ---");
    let processing_start_time = Instant::now();

    let progress_bar = ProgressBar::new(files.len() as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta} @ {per_sec}) {msg}")
            .expect("Failed to create progress bar template")
            .progress_chars("=> "),
    );
    progress_bar.set_message("Starting...");

    let stats_for_map = Arc::clone(&stats);
    let output_dir_clone = output_dir.clone();

    let processing_results: Result<FileStats, Vec<String>> = {
        let results: Vec<Result<FileStats, String>> = files
            .par_iter()
            .map(|filepath| {
                let pb_clone = progress_bar.clone();
                let stats_clone = Arc::clone(&stats_for_map);
                let output_dir_ref = &output_dir_clone;
                process_file_for_index(filepath, output_dir_ref, &pb_clone, stats_clone)
            })
            .collect();

        let mut aggregated_stats = FileStats::default();
        let mut errors: Vec<String> = Vec::new();
        let mut ok_count = 0;

        for result in results {
            match result {
                Ok(file_stats) => {
                    aggregated_stats.lines_read += file_stats.lines_read;
                    aggregated_stats.json_parse_errors += file_stats.json_parse_errors;
                    aggregated_stats.member_id_missing += file_stats.member_id_missing;
                    aggregated_stats.prefix_missing += file_stats.prefix_missing;
                    aggregated_stats.doi_missing += file_stats.doi_missing;
                    ok_count += 1;
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

         progress_bar.println(format!("Parallel processing map phase completed. {} Ok, {} Err.", ok_count, errors.len()));


        if errors.is_empty() {
             Ok(aggregated_stats)
        } else {
            Err(errors)
        }
    };


    progress_bar.finish_with_message("Processing finished.");
    let processing_duration = processing_start_time.elapsed();

    #[allow(unused_assignments)]
    let mut processing_successful = false;

    match processing_results {
        Ok(final_file_stats_aggregate) => {
             info!("--- Processing Pass Summary ---");
             info!("Duration: {}", format_elapsed(processing_duration));
             info!("Input files processed: {}", files.len());
             info!("(Aggregated local stats: {} lines, {} parse errors)", final_file_stats_aggregate.lines_read, final_file_stats_aggregate.json_parse_errors);

            info!("--- Starting Concatenation Pass ---");
            let concat_start_time = Instant::now();
            let mut concat_errors: Vec<String> = Vec::new();
            let mut final_counts = (0u64, 0u64, 0u64);


            let member_csv_path = output_dir.join("member_index.csv");
            match concatenate_partial_files(
                &output_dir,
                ".member.part",
                &member_csv_path,
                &["member_id", "input_file"],
            ) {
                Ok(count) => final_counts.0 = count,
                Err(e) => concat_errors.push(format!("Member concatenation failed: {}", e)),
            }


             let prefix_csv_path = output_dir.join("prefix_index.csv");
             match concatenate_partial_files(
                 &output_dir,
                 ".prefix.part",
                 &prefix_csv_path,
                 &["member_id", "prefix", "input_file"],
             ) {
                 Ok(count) => final_counts.1 = count,
                 Err(e) => concat_errors.push(format!("Prefix concatenation failed: {}", e)),
             }


             let doi_csv_path = output_dir.join("doi_index.csv");
              match concatenate_partial_files(
                  &output_dir,
                  ".doi.part",
                  &doi_csv_path,
                  &["doi", "member_id", "prefix", "input_file"],
              ) {
                  Ok(count) => final_counts.2 = count,
                  Err(e) => concat_errors.push(format!("DOI concatenation failed: {}", e)),
              }

            let concat_duration = concat_start_time.elapsed();

             if concat_errors.is_empty() {
                info!("Finished concatenating files in {}", format_elapsed(concat_duration));
                  let final_stats_snapshot = FileStats {
                        lines_read: stats.total_lines_read.load(Ordering::Relaxed),
                        json_parse_errors: stats.total_json_parse_errors.load(Ordering::Relaxed),
                        member_id_missing: stats.total_member_id_missing.load(Ordering::Relaxed),
                        prefix_missing: stats.total_prefix_missing.load(Ordering::Relaxed),
                        doi_missing: stats.total_doi_missing.load(Ordering::Relaxed),
                    };
                 stats.update_from_final(&final_stats_snapshot, final_counts);
                 processing_successful = true;
                 stats.log_final_stats(true);

             } else {
                 error!("--- Concatenation Pass Failed ---");
                 for err in concat_errors {
                     error!("  - {}", err);
                 }
                  stats.log_final_stats(false);
                 signal_stats_thread_stop(&stats_thread_running);
                 return Err(anyhow::anyhow!("Concatenation failed. Partial results may exist."));
             }


        }
        Err(errors) => {
             error!("--- Processing Pass Failed ---");
             error!("Errors occurred during file processing:");
             for error_msg in errors {
                 error!("  - {}", error_msg);
             }
              stats.log_final_stats(false);

             signal_stats_thread_stop(&stats_thread_running);
             return Err(anyhow::anyhow!("Indexing failed due to processing errors."));
         }
    }


    signal_stats_thread_stop(&stats_thread_running);
    info!("Waiting for stats thread to finish...");
    if let Err(e) = stats_thread.join() {
        error!("Error joining stats thread: {:?}", e);
    } else {
        info!("Stats thread joined successfully.");
    }

    let total_runtime = main_start_time.elapsed();
    info!("-------------------- FINAL SUMMARY --------------------");
    info!("Total execution time: {}", format_elapsed(total_runtime));
    stats.log_final_stats(processing_successful); // Log final stats
    memory_usage::log_memory_usage("final");
    if processing_successful {
        info!("Indexing process finished successfully.");
    } else {
         error!("Indexing process finished with errors.");
    }
    info!("-------------------------------------------------------");

    if processing_successful {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Indexing finished with concatenation errors."))
    }
}

fn signal_stats_thread_stop(running_flag: &Arc<Mutex<bool>>) {
     info!("Signaling stats thread to stop...");
     match running_flag.lock() {
         Ok(mut guard) => *guard = false,
         Err(e) => error!("Failed to lock stats thread running flag to signal stop: {}", e),
     }
}
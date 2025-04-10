use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle, ParallelProgressIterator};
use log::{debug, error, info, warn, LevelFilter};
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use rayon::prelude::*;
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
        let command = format!("process where ProcessId={} get WorkingSetSize, VirtualSize", pid);
        let wmic_output = Command::new("wmic")
                             .arg(&command)
                             .arg("/value")
                             .output()
                             .ok()?;

        let output_str = String::from_utf8_lossy(&wmic_output.stdout);
        let mut rss_bytes = None;
        let mut vm_size_bytes = None;

        for line in output_str.lines() {
            if let Some((key, value)) = line.trim().split_once('=') {
                match key.trim() {
                    "VirtualSize" => vm_size_bytes = value.trim().parse::<f64>().ok(),
                    "WorkingSetSize" => rss_bytes = value.trim().parse::<f64>().ok(),
                    _ => {}
                }
            }
             if rss_bytes.is_some() && vm_size_bytes.is_some() {
                 break;
             }
        }

        let rss_mb = rss_bytes.unwrap_or(0.0) / (1024.0 * 1024.0);
        let vm_size_mb = vm_size_bytes.unwrap_or(0.0) / (1024.0 * 1024.0);
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
                     if total_bytes > 0.0 && rss_bytes.is_some() {
                        percent = Some((rss_bytes.unwrap() / total_bytes) * 100.0);
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
                 if cfg!(target_os = "windows") && stats.rss_mb > 0.0 {
                     "".to_string()
                 } else {
                     "N/A virtual".to_string()
                 }
            };
            let comma = if !vm_str.is_empty() { ", " } else { "" };
            if stats.rss_mb > 0.0 || stats.vm_size_mb > 0.0 {
                info!(
                    "Memory usage ({}): {:.1} MB physical (RSS){}{} {} of system memory",
                    note, stats.rss_mb, comma, vm_str, percent_str
                );
            } else if stats.percent.is_none() {
                 info!("Memory usage tracking not available or failed on this platform ({})", std::env::consts::OS);
            }

        } else {
            info!(
                "Memory usage tracking not available or failed on this platform ({})",
                std::env::consts::OS
            );
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "Crossref Data File Sampler")]
#[command(about = "Randomly samples lines from Crossref Data File JSONL.gz files in a directory.")]
#[command(version = "1.0.0")]
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
        help = "Path to the output file where sampled lines will be written",
        required = true
    )]
    output_file: String,

    #[arg(
        short = 'n',
        long,
        help = "Number of lines to sample",
        required = true
    )]
    sample_count: u64,

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
        long,
        help = "Don't shuffle the final sample (faster, but output order depends on processing order)"
    )]
    no_shuffle: bool,
}

type SampleResult = Vec<String>;

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

fn count_lines_in_file(filepath: &PathBuf) -> Result<u64> {
    let file = File::open(filepath)
        .with_context(|| format!("Failed to open {} for counting", filepath.display()))?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);
    let mut line_count = 0u64;
    for line_result in reader.lines() {
         match line_result {
            Ok(_) => line_count += 1,
            Err(e) => {
                return Err(e).with_context(|| format!("Failed reading line during count in {}", filepath.display()));
            }
        }
    }
    Ok(line_count)
}

fn sample_lines_from_file(filepath: &PathBuf, num_samples_for_file: u64) -> Result<SampleResult> {
    if num_samples_for_file == 0 {
        return Ok(Vec::new());
    }

    let file = File::open(filepath)
        .with_context(|| format!("Failed to open {} for sampling", filepath.display()))?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut reservoir: Vec<String> = Vec::with_capacity(num_samples_for_file as usize);
    let mut lines_seen = 0u64;
    let mut rng = thread_rng();

    for line_result in reader.lines() {
        let line = line_result.with_context(|| format!("Failed reading line during sampling in {}", filepath.display()))?;
        lines_seen += 1;

        if reservoir.len() < (num_samples_for_file as usize) {
            reservoir.push(line);
        } else {
            let j = rng.gen_range(0..lines_seen);
            if (j as usize) < reservoir.len() {
                reservoir[j as usize] = line;
            }
        }
    }

    if lines_seen < num_samples_for_file {
        warn!("File {} had fewer lines ({}) than requested samples ({}), taking all lines.",
              filepath.display(), lines_seen, num_samples_for_file);
    }

    Ok(reservoir)
}

fn calculate_samples_per_file(
    file_counts: &[(PathBuf, u64)],
    total_lines: u64,
    total_samples_needed: u64,
) -> Result<Vec<(PathBuf, u64)>> {
    if total_lines == 0 {
        info!("Total line count is 0. No samples can be taken.");
        return Ok(Vec::new());
    }
    if total_samples_needed == 0 {
        info!("Requested sample count is 0.");
        return Ok(Vec::new());
    }
    if total_samples_needed > total_lines {
        warn!(
            "Requested sample count ({}) exceeds total lines found ({}). Sampling all lines.",
            total_samples_needed, total_lines
        );
        return Ok(file_counts
            .iter()
            .map(|(path, count)| (path.clone(), *count))
            .collect());
    }

    let mut samples_per_file = Vec::with_capacity(file_counts.len());
    let mut current_total_samples = 0u64;

    let mut file_targets: Vec<(PathBuf, u64, f64, f64)> = file_counts
        .iter()
        .map(|(path, count)| {
            let target = (*count as f64 / total_lines as f64) * total_samples_needed as f64;
            let fractional_part = target.fract();
            (path.clone(), *count, target, fractional_part)
        })
        .collect();

    for (path, count, target, _) in &file_targets {
        let base_samples = target.floor() as u64;
        let samples_to_take = base_samples.min(*count);
        samples_per_file.push((path.clone(), samples_to_take));
        current_total_samples += samples_to_take;
    }

    let mut samples_remaining_to_allocate = total_samples_needed.saturating_sub(current_total_samples);

    if samples_remaining_to_allocate > 0 {
        file_targets.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

        for (path_sorted, count_sorted, _, _) in file_targets {
             if samples_remaining_to_allocate == 0 {
                 break;
             }
            if let Some(entry) = samples_per_file.iter_mut().find(|(p, _)| p == &path_sorted) {
                if entry.1 < count_sorted { // Removed *
                     entry.1 += 1;
                     samples_remaining_to_allocate -= 1;
                }
            }
        }
    }

    let final_allocated: u64 = samples_per_file.iter().map(|(_, count)| count).sum();
    if final_allocated != total_samples_needed {
         warn!("Final allocated sample count ({}) differs slightly from requested ({}) due to file sizes and rounding. Adjusting...", final_allocated, total_samples_needed);
    }

    info!("Calculated sample distribution across {} files.", samples_per_file.len());
    Ok(samples_per_file)
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

    info!("Starting JSONL Sampler v1.0.0");
    memory_usage::log_memory_usage("initial");

    let input_dir = PathBuf::from(&cli.input_dir);
    let output_file_path = PathBuf::from(&cli.output_file);
    let samples_needed = cli.sample_count;

    if samples_needed == 0 {
        info!("Sample count is 0. Exiting.");
        File::create(&output_file_path)?.flush()?;
         info!("Created empty output file: {}", output_file_path.display());
        return Ok(());
    }

    if let Some(parent_dir) = output_file_path.parent() {
        fs::create_dir_all(parent_dir)
            .with_context(|| format!("Failed to create output directory: {}", parent_dir.display()))?;
    }

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
    info!("Output file: {}", output_file_path.display());
    info!("Lines to sample: {}", samples_needed);
    info!("Shuffle output: {}", !cli.no_shuffle);

    let files = find_jsonl_gz_files(&input_dir)?;
    if files.is_empty() {
        warn!("No .jsonl.gz files found. Exiting.");
        return Ok(());
    }
    info!("Found {} input files.", files.len());

    info!("--- Starting Phase 1: Counting total lines ---");
    let count_start_time = Instant::now();
    let count_pb = ProgressBar::new(files.len() as u64);
    count_pb.set_style(
        ProgressStyle::default_bar()
             .template("[{elapsed_precise}] Counting: [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
             .expect("Failed to create progress bar template")
             .progress_chars("=> "),
    );

    let file_counts_results: Vec<Result<(PathBuf, u64)>> = files
        .par_iter()
        .progress_with(count_pb.clone())
        .map(|filepath| {
             count_pb.set_message(filepath.file_name().unwrap_or_default().to_string_lossy().into_owned());
             count_lines_in_file(filepath).map(|count| (filepath.clone(), count))
         })
        .collect();

    count_pb.finish_with_message("Counting complete.");

    let mut file_counts: Vec<(PathBuf, u64)> = Vec::with_capacity(files.len());
    let mut total_lines: u64 = 0;
    let mut count_errors: Vec<String> = Vec::new();

    for result in file_counts_results {
        match result {
            Ok((path, count)) => {
                if count > 0 {
                     file_counts.push((path, count));
                     total_lines += count;
                } else {
                    debug!("File {} has 0 lines, skipping.", path.display());
                }
            }
            Err(e) => {
                error!("Error counting lines: {}", e);
                count_errors.push(e.to_string());
            }
        }
    }

    if !count_errors.is_empty() {
         error!("Errors occurred during line counting phase. Cannot proceed.");
         return Err(anyhow::anyhow!("Line counting failed for one or more files."));
    }

     if total_lines == 0 {
        info!("No lines found in any input files. Exiting.");
        File::create(&output_file_path)?.flush()?;
        info!("Created empty output file: {}", output_file_path.display());
        return Ok(());
    }

    info!(
        "Count phase completed in {}. Total lines found: {}",
        format_elapsed(count_start_time.elapsed()),
        total_lines
    );
    memory_usage::log_memory_usage("after counting");

    let samples_per_file_list =
        calculate_samples_per_file(&file_counts, total_lines, samples_needed)?;

    let files_to_sample: Vec<(PathBuf, u64)> = samples_per_file_list
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .collect();

     if files_to_sample.is_empty() && samples_needed > 0 {
         warn!("Calculated 0 samples to take from any file, though {} were requested. This might happen if sample size is very small compared to file count/distribution.", samples_needed);
         File::create(&output_file_path)?.flush()?;
         info!("Created empty output file: {}", output_file_path.display());
         return Ok(());
     }

    info!("--- Starting Phase 2: Sampling lines ---");
    let sample_start_time = Instant::now();
    let sample_pb = ProgressBar::new(files_to_sample.len() as u64);
     sample_pb.set_style(
        ProgressStyle::default_bar()
             .template("[{elapsed_precise}] Sampling: [{bar:40.green/blue}] {pos}/{len} ({eta}) {msg}")
             .expect("Failed to create progress bar template")
             .progress_chars("=> "),
    );

    let sample_results: Vec<Result<SampleResult>> = files_to_sample
        .par_iter()
        .progress_with(sample_pb.clone())
        .map(|(filepath, num_samples)| {
            sample_pb.set_message(filepath.file_name().unwrap_or_default().to_string_lossy().into_owned());
            sample_lines_from_file(filepath, *num_samples)
        })
        .collect();

     sample_pb.finish_with_message("Sampling complete.");

    let mut all_sampled_lines: Vec<String> = Vec::with_capacity(samples_needed as usize);
    let mut sample_errors: Vec<String> = Vec::new();

    for result in sample_results {
        match result {
            Ok(mut lines) => {
                all_sampled_lines.append(&mut lines);
            }
            Err(e) => {
                error!("Error sampling lines: {}", e);
                sample_errors.push(e.to_string());
            }
        }
    }

     if !sample_errors.is_empty() {
         error!("Errors occurred during sampling phase. Output might be incomplete.");
         return Err(anyhow::anyhow!("Sampling failed for one or more files."));
     }

    info!(
        "Sampling phase completed in {}. Collected {} lines (requested {}).",
        format_elapsed(sample_start_time.elapsed()),
        all_sampled_lines.len(),
        samples_needed
    );
    memory_usage::log_memory_usage("after sampling");

    if !cli.no_shuffle {
        info!("Shuffling {} sampled lines...", all_sampled_lines.len());
        let shuffle_start = Instant::now();
        let mut rng = thread_rng();
        all_sampled_lines.shuffle(&mut rng);
        info!("Shuffling took {}", format_elapsed(shuffle_start.elapsed()));
    } else {
        info!("Skipping final shuffle.");
    }

    info!("Writing {} sampled lines to {}", all_sampled_lines.len(), output_file_path.display());
    let write_start_time = Instant::now();

    let output_file = File::create(&output_file_path)
        .with_context(|| format!("Failed to create output file: {}", output_file_path.display()))?;
    let mut writer = BufWriter::new(output_file);

    let write_pb = ProgressBar::new(all_sampled_lines.len() as u64);
     write_pb.set_style(
        ProgressStyle::default_bar()
             .template("[{elapsed_precise}] Writing:  [{bar:40.yellow/red}] {pos}/{len} ({eta})")
             .expect("Failed to create progress bar template")
             .progress_chars("=> "),
    );

    for line in all_sampled_lines.iter() {
        writeln!(writer, "{}", line)
            .with_context(|| format!("Failed to write line to output file: {}", output_file_path.display()))?;
         write_pb.inc(1);
    }

    writer.flush().context("Failed to flush output writer")?;
    write_pb.finish_with_message("Write complete.");

    info!("Finished writing output file in {}", format_elapsed(write_start_time.elapsed()));
    memory_usage::log_memory_usage("final");

    let total_runtime = main_start_time.elapsed();
    info!("-------------------- FINAL SUMMARY --------------------");
    info!("Total execution time: {}", format_elapsed(total_runtime));
    info!("Input files processed: {}", files.len());
    info!("Total lines found: {}", total_lines);
    info!("Lines sampled: {}", all_sampled_lines.len());
    info!("Output file: {}", output_file_path.display());
    info!("-------------------------------------------------------");

    Ok(())
}
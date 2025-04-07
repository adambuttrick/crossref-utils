use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
        Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use simple_logger::SimpleLogger;
use time::macros::format_description;

#[cfg(target_os = "linux")]
use std::fs::read_to_string;
#[cfg(target_os = "windows")]
use std::process::Command as WinCommand;

#[derive(Parser)]
#[command(name = "Crossref Reorganizer")]
#[command(about = "Reorganizes Crossref JSONL.gz files by member ID and DOI prefix, with optional filtering.")]
#[command(version = "1.1.1")]
struct Cli {
    #[arg(short, long, help = "Directory containing input JSONL.gz files", required = true)]
    input: String,

    #[arg(short, long, help = "Base directory for organized output structure", required = true)]
    output_dir: String,

    #[arg(long, help = "Temporary directory for intermediate files (defaults to <output_dir>/_cr_temp)")]
    temp_dir: Option<String>,

    #[arg(long, default_value = "256", help = "Max open intermediate member files (Pass 1)")]
    max_intermediate_files: usize,

    #[arg(long, default_value = "128", help = "Max open final prefix files per member (Pass 2)")]
    max_final_files_per_member: usize,

    #[arg(short, long, default_value = "INFO", help = "Logging level (DEBUG, INFO, WARN, ERROR)")]
    log_level: String,

    #[arg(short, long, default_value = "0", help = "Number of threads to use (0 for auto)")]
    threads: usize,

    #[arg(long, help = "Path to a JSON file specifying filters (members and optional prefixes)")]
    filter_file: Option<String>,

    #[arg(short, long, default_value = "60", help = "Interval in seconds to log statistics")]
    stats_interval: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(transparent)]
struct MemberId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(transparent)]
struct DoiPrefix(String);

type FilterFileConfig = HashMap<String, Option<Vec<String>>>;
type FilterConfig = Arc<HashMap<MemberId, (Option<HashSet<DoiPrefix>>, Vec<Vec<u8>>)>>;


#[derive(Default)]
struct Stats {
    pass1_lines_read: AtomicU64,
    pass1_lines_written: AtomicU64,
    pass1_json_parse_errors: AtomicU64,
    pass1_member_id_missing: AtomicU64,
    pass1_lines_skipped_by_filter: AtomicU64,
    pass1_lines_pre_filtered: AtomicU64,
    pass1_intermediate_files_opened: AtomicU64,
    pass1_unique_members_found: Mutex<HashSet<MemberId>>,

    pass2_members_processed: AtomicU64,
    pass2_lines_read: AtomicU64,
    pass2_lines_written: AtomicU64,
    pass2_json_parse_errors: AtomicU64,
    pass2_prefix_missing: AtomicU64,
    pass2_lines_skipped_by_filter: AtomicU64,
    pass2_final_files_opened: AtomicU64,
    pass2_unique_prefixes_total: Mutex<HashSet<(MemberId, DoiPrefix)>>,
}

impl Stats {
    fn new() -> Self {
        Default::default()
    }

    fn log_current_stats(&self, stage: &str) {
        let pass1_unique_members_count = self.pass1_unique_members_found.lock().unwrap().len();
        let pass2_unique_prefixes_count = self.pass2_unique_prefixes_total.lock().unwrap().len();

        let pass1_lines_read = self.pass1_lines_read.load(Ordering::Relaxed);
        let pass1_lines_written = self.pass1_lines_written.load(Ordering::Relaxed);
        let pass1_lines_skipped_filter = self.pass1_lines_skipped_by_filter.load(Ordering::Relaxed);
        let pass1_lines_pre_filtered = self.pass1_lines_pre_filtered.load(Ordering::Relaxed);
        let pass1_json_errors = self.pass1_json_parse_errors.load(Ordering::Relaxed);
        let pass1_member_missing = self.pass1_member_id_missing.load(Ordering::Relaxed);
        let pass1_files_opened = self.pass1_intermediate_files_opened.load(Ordering::Relaxed);

        let pass2_members_processed = self.pass2_members_processed.load(Ordering::Relaxed);
        let pass2_lines_read = self.pass2_lines_read.load(Ordering::Relaxed);
        let pass2_lines_written = self.pass2_lines_written.load(Ordering::Relaxed);
        let pass2_lines_skipped_filter = self.pass2_lines_skipped_by_filter.load(Ordering::Relaxed);
        let pass2_json_errors = self.pass2_json_parse_errors.load(Ordering::Relaxed);
        let pass2_prefix_missing = self.pass2_prefix_missing.load(Ordering::Relaxed);
        let pass2_files_opened = self.pass2_final_files_opened.load(Ordering::Relaxed);

        info!("--- Periodic Stats ({}) ---", stage);
        if stage == "Pass 1" || stage == "Final" {
            info!(" Pass 1:");
            info!("    Lines Read: {}", pass1_lines_read);
            info!("    Lines Pre-Filtered (Bytes): {}", pass1_lines_pre_filtered);
            info!("    Lines Written (Intermediate): {}", pass1_lines_written);
            info!("    Lines Skipped by Member Filter: {}", pass1_lines_skipped_filter);
            info!("    JSON Parse Errors: {}", pass1_json_errors);
            info!("    Member ID Missing (Post-Parse): {}", pass1_member_missing);
            info!("    Unique Members Written (so far): {}", pass1_unique_members_count);
            info!("    Intermediate Files Opened (cumulative): {}", pass1_files_opened);
        }
        if stage == "Pass 2" || stage == "Final" {
            info!(" Pass 2:");
            info!("    Members Processed: {}", pass2_members_processed);
            info!("    Lines Read (Intermediate): {}", pass2_lines_read);
            info!("    Lines Written (Final): {}", pass2_lines_written);
            info!("    Lines Skipped by Prefix Filter: {}", pass2_lines_skipped_filter);
            info!("    JSON Parse Errors: {}", pass2_json_errors);
            info!("    Prefix Missing: {}", pass2_prefix_missing);
            info!("    Unique Member/Prefix Pairs Written: {}", pass2_unique_prefixes_count);
            info!("    Final Output Files Opened (cumulative): {}", pass2_files_opened);
        }
        info!("------------------------------");
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

#[derive(Deserialize)]
struct Pass1Record {
    member: Option<serde_json::Value>,
}

fn extract_member_id_from_value(member_value: &Option<Value>) -> Option<MemberId> {
    member_value.as_ref().and_then(|v| {
        if v.is_string() {
            v.as_str().map(|s| MemberId(s.to_string()))
        } else if v.is_number() {
            Some(MemberId(v.to_string()))
        } else {
            None
        }
    })
}

#[derive(Deserialize)]
struct Pass2Record {
    prefix: Option<String>,
    #[serde(rename = "DOI")]
    doi: Option<String>,
}

fn extract_doi_prefix_from_record(record: &Pass2Record) -> Option<DoiPrefix> {
    record.prefix.as_ref().map(|s| DoiPrefix(s.clone()))
        .or_else(|| {
            record.doi.as_ref()
                .and_then(|doi_str| doi_str.split_once('/'))
                .map(|(pfx, _)| DoiPrefix(pfx.to_string()))
        })
}

#[inline]
fn contains_any_pattern(bytes: &[u8], patterns: &[Vec<u8>]) -> bool {
    for pattern in patterns {
        if bytes.windows(pattern.len()).any(|window| window == pattern) {
            return true;
        }
    }
    false
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
                 vm_rss_kb = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok());
             } else if line.starts_with("VmSize:") {
                 vm_size_kb = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok());
             }
             if vm_rss_kb.is_some() && vm_size_kb.is_some() {
                 break;
             }
         }

         let rss_mb = vm_rss_kb? / 1024.0;
         let vm_size_mb = vm_size_kb? / 1024.0;
         let mut percent = None;

         if let Ok(meminfo) = read_to_string("/proc/meminfo") {
             if let Some(mem_total_kb) = meminfo.lines()
                 .find(|line| line.starts_with("MemTotal:"))
                 .and_then(|line| line.split_whitespace().nth(1))
                 .and_then(|s| s.parse::<f64>().ok()) {
                 if mem_total_kb > 0.0 {
                     percent = Some((vm_rss_kb.unwrap_or(0.0) / mem_total_kb) * 100.0);
                 }
             }
         }

         Some(MemoryStats { rss_mb, vm_size_mb, percent })
     }

     #[cfg(target_os = "macos")]
     pub fn get_memory_usage() -> Option<MemoryStats> {
         use std::process::Command;

         let pid = std::process::id();
         let ps_output = Command::new("ps")
             .args(&["-o", "rss=", "-p", &pid.to_string()])
             .output().ok()?;
         let rss_kb = String::from_utf8_lossy(&ps_output.stdout).trim().parse::<f64>().ok()?;

         let vsz_output = Command::new("ps")
             .args(&["-o", "vsz=", "-p", &pid.to_string()])
             .output().ok()?;
         let vsz_kb = String::from_utf8_lossy(&vsz_output.stdout).trim().parse::<f64>().ok()?;

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

         Some(MemoryStats { rss_mb, vm_size_mb, percent })
     }

     #[cfg(target_os = "windows")]
     pub fn get_memory_usage() -> Option<MemoryStats> {
         use std::process::Command;

         let pid = std::process::id();
         let tasklist_output = Command::new("tasklist")
             .args(&["/fi", &format!("PID eq {}", pid), "/fo", "csv", "/nh"])
             .output().ok()?;

         let output_str = String::from_utf8_lossy(&tasklist_output.stdout);
         let mem_usage_str = output_str.rsplit(',').next()?
                                       .trim().trim_matches('"');
         let rss_kb = mem_usage_str.trim_end_matches(" K").replace(',', "").parse::<f64>().ok()?;

         let vm_size_mb = 0.0;
         let rss_mb = rss_kb / 1024.0;
         let mut percent = None;

         if let Ok(mem_output) = Command::new("wmic")
             .args(&["ComputerSystem", "get", "TotalPhysicalMemory", "/value"])
             .output() {
             let mem_str = String::from_utf8_lossy(&mem_output.stdout);
             if let Some(total_bytes_str) = mem_str.lines()
                 .find(|line| line.starts_with("TotalPhysicalMemory="))
                 .and_then(|line| line.split('=').nth(1)) {
                 if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() {
                     let total_kb = total_bytes / 1024.0;
                     if total_kb > 0.0 {
                         percent = Some((rss_kb / total_kb) * 100.0);
                     }
                 }
             }
         }

         Some(MemoryStats { rss_mb, vm_size_mb, percent })
     }

     #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
     pub fn get_memory_usage() -> Option<MemoryStats> {
         None
     }

     pub fn log_memory_usage(note: &str) {
         if let Some(stats) = get_memory_usage() {
             let percent_str = stats.percent.map_or_else(|| "N/A".to_string(), |p| format!("{:.1}%", p));
             let vm_str = if stats.vm_size_mb > 0.0 { format!("{:.1} MB virtual", stats.vm_size_mb) } else { "N/A virtual".to_string() };
             info!(
                 "Memory usage ({}): {:.1} MB physical (RSS), {}, {} of system memory",
                 note, stats.rss_mb, vm_str, percent_str
             );
         } else {
             info!("Memory usage tracking not available or failed on this platform ({})", std::env::consts::OS);
         }
     }
}

struct IntermediateWriterManager {
    base_temp_dir: PathBuf,
    max_open: usize,
    writers: Mutex<HashMap<MemberId, BufWriter<File>>>,
    lru: Mutex<VecDeque<MemberId>>,
    created_dirs: Mutex<HashSet<PathBuf>>,
    files_opened_counter: AtomicU64,
}

impl IntermediateWriterManager {
    fn new(base_temp_dir: PathBuf, max_open: usize) -> Result<Self> {
        if base_temp_dir.exists() {
            info!("Cleaning existing temporary directory: {}", base_temp_dir.display());
            fs::remove_dir_all(&base_temp_dir)
                .with_context(|| format!("Failed to remove existing temp directory: {}", base_temp_dir.display()))?;
        }
        fs::create_dir_all(&base_temp_dir)
            .with_context(|| format!("Failed to create base temp directory: {}", base_temp_dir.display()))?;

        Ok(Self {
            base_temp_dir,
            max_open: max_open.max(1),
            writers: Mutex::new(HashMap::new()),
            lru: Mutex::new(VecDeque::new()),
            created_dirs: Mutex::new(HashSet::new()),
            files_opened_counter: AtomicU64::new(0),
        })
    }

    fn get_files_opened_count(&self) -> u64 {
        self.files_opened_counter.load(Ordering::Relaxed)
    }

    fn write_line(&self, member_id: &MemberId, line: &str) -> Result<()> {
        let mut writers_guard = self.writers.lock().unwrap();
        let mut lru_guard = self.lru.lock().unwrap();

        let was_present = writers_guard.contains_key(member_id);

        // Evict LRU if needed (BEFORE trying to insert)
        if !was_present && writers_guard.len() >= self.max_open {
             // Keep evicting until below limit
             while writers_guard.len() >= self.max_open {
                 if let Some(evict_id) = lru_guard.pop_back() {
                    debug!("Evicting intermediate writer for member {}", evict_id.0);
                    if let Some(mut writer_to_close) = writers_guard.remove(&evict_id) {
                        // Drop guards *before* flushing evicted writer to reduce lock contention
                        drop(lru_guard);
                        drop(writers_guard);
                        if let Err(e) = writer_to_close.flush() {
                            warn!("Error flushing evicted intermediate writer for member {}: {}", evict_id.0, e);
                        }
                        // Re-acquire locks for the next loop iteration or the subsequent entry call
                        writers_guard = self.writers.lock().unwrap();
                        lru_guard = self.lru.lock().unwrap();
                    } else {
                        warn!("LRU contained evicted member ID {} but it wasn't in the writers map.", evict_id.0);
                    }
                } else {
                    error!("LRU queue is empty while trying to evict but writers map is full. Max open: {}", self.max_open);
                    break; // Avoid infinite loops
                }
             }
        }

        // Use entry API to handle getting existing or inserting new writer
        let writer = writers_guard
            .entry(member_id.clone())
            .or_insert_with(|| {
                // Create New Writer
                let member_dir = self.base_temp_dir.join(&member_id.0);
                {
                    // No mut needed here, only calling contains
                    let created_dirs_guard = self.created_dirs.lock().unwrap();
                    if !created_dirs_guard.contains(&member_dir) {
                        drop(created_dirs_guard);
                        match fs::create_dir_all(&member_dir) {
                           Ok(_) => {
                               debug!("Created intermediate directory: {}", member_dir.display());
                               self.created_dirs.lock().unwrap().insert(member_dir.clone());
                           },
                           Err(e) => {
                               error!("Failed to create intermediate directory {}: {}", member_dir.display(), e);
                           }
                        }
                    }
                }

                let part_filename = format!("part_{}.jsonl", uuid::Uuid::new_v4());
                let file_path = member_dir.join(part_filename);

                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(&file_path)
                    .expect(&format!("Failed to open/create intermediate file: {}", file_path.display()));

                debug!("Opened intermediate file: {}", file_path.display());
                self.files_opened_counter.fetch_add(1, Ordering::Relaxed);
                lru_guard.push_front(member_id.clone());

                BufWriter::new(file)
            });

        // Update LRU for existing entries
        if was_present {
            if lru_guard.front() != Some(member_id) {
                if let Some(pos) = lru_guard.iter().position(|id| id == member_id) {
                    let id = lru_guard.remove(pos).unwrap();
                    lru_guard.push_front(id);
                } else {
                    warn!("Member {} writer existed but wasn't in LRU queue? Adding it.", member_id.0);
                    lru_guard.push_front(member_id.clone());
                }
            }
        }

        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;

        Ok(())
    }

    fn flush_all(&self) -> Result<()> {
        info!("Flushing all intermediate writers...");
        let mut writers_guard = self.writers.lock().unwrap();
        let mut errors = Vec::new();

        for (member_id, writer) in writers_guard.iter_mut() {
            if let Err(e) = writer.flush() {
                let msg = format!("Failed to flush intermediate writer for member {}: {}", member_id.0, e);
                error!("{}", msg);
                errors.push(msg);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Errors occurred during intermediate flush:\n - {}", errors.join("\n - ")))
        }
    }
}

impl Drop for IntermediateWriterManager {
    fn drop(&mut self) {
        info!("IntermediateWriterManager dropping. Attempting final flush...");
        if let Err(e) = self.flush_all() {
            error!("Error during final flush in IntermediateWriterManager drop: {}", e);
        }
    }
}

fn process_member_directory(
    member_dir_path: PathBuf,
    final_output_base_dir: PathBuf,
    max_final_files_per_member: usize,
    global_stats: Arc<Stats>,
    filter_config: Option<FilterConfig>,
) -> Result<()> {
    let member_id_str = member_dir_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("Could not extract member ID from path: {}", member_dir_path.display()))?
        .to_string();
    let member_id = MemberId(member_id_str.clone());
    let start_time = Instant::now();

    debug!("[Member {}] Starting Pass 2 processing", member_id.0);

    let member_filter_prefixes: Option<HashSet<DoiPrefix>> = filter_config
        .as_ref()
        .and_then(|filters| filters.get(&member_id))
        .and_then(|(opt_set, _)| opt_set.as_ref())
        .cloned();

    let final_member_dir = final_output_base_dir.join(&member_id.0);
    fs::create_dir_all(&final_member_dir)
        .with_context(|| format!("[Member {}] Failed to create final output directory: {}", member_id.0, final_member_dir.display()))?;

    let mut writers: HashMap<DoiPrefix, BufWriter<GzEncoder<File>>> = HashMap::new();
    let mut lru: VecDeque<DoiPrefix> = VecDeque::with_capacity(max_final_files_per_member);
    let mut lines_read_count = 0u64;
    let mut lines_written_count = 0u64;
    let mut parse_error_count = 0u64;
    let mut prefix_missing_count = 0u64;
    let mut skipped_by_filter_count = 0u64;
    let mut unique_prefixes_in_member = HashSet::new();
    let mut final_files_opened_count = 0u64;

    let part_files = glob(&member_dir_path.join("part_*.jsonl").to_string_lossy())?
        .filter_map(Result::ok)
        .collect::<Vec<_>>();

    if part_files.is_empty() {
        warn!("[Member {}] No intermediate part files found in {}. Skipping.", member_id.0, member_dir_path.display());
        if let Err(e) = fs::remove_dir(&member_dir_path) {
             warn!("[Member {}] Failed to remove empty temp directory {}: {}", member_id.0, member_dir_path.display(), e);
        }
        global_stats.pass2_members_processed.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }

    for part_file_path in part_files {
        debug!("[Member {}] Processing part file: {}", member_id.0, part_file_path.display());
        let file = File::open(&part_file_path)
            .with_context(|| format!("[Member {}] Failed to open part file: {}", member_id.0, part_file_path.display()))?;
        let reader = BufReader::new(file);

        for line_result in reader.lines() {
            lines_read_count += 1;
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    warn!("[Member {}] Error reading line from {}: {}", member_id.0, part_file_path.display(), e);
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            let (prefix, line_content) = match serde_json::from_str::<Pass2Record>(&line) {
                 Ok(record) => {
                     match extract_doi_prefix_from_record(&record) {
                         Some(p) => (p, line),
                         None => {
                             prefix_missing_count += 1;
                             (DoiPrefix("_unknown_".to_string()), line)
                         }
                     }
                 },
                 Err(e) => {
                     warn!("[Member {}] Failed to parse JSON in line from {}: {} (Line: {})", member_id.0, part_file_path.display(), e, line.chars().take(100).collect::<String>());
                     parse_error_count += 1;
                     continue;
                 }
             };

             let should_process = match member_filter_prefixes {
                 Some(ref allowed_prefixes) => allowed_prefixes.contains(&prefix),
                 None => true,
             };

            if !should_process {
                skipped_by_filter_count += 1;
                continue;
            }

            if !writers.contains_key(&prefix) {
                while writers.len() >= max_final_files_per_member {
                    if let Some(evict_prefix) = lru.pop_back() {
                        debug!("[Member {}] Evicting final writer for prefix {}", member_id.0, evict_prefix.0);
                        if let Some(mut writer_to_close) = writers.remove(&evict_prefix) {
                            if let Err(e) = writer_to_close.flush() {
                                warn!("[Member {}] Error flushing evicted final writer buffer for prefix {}: {}", member_id.0, evict_prefix.0, e);
                            }
                            match writer_to_close.into_inner() {
                                Ok(gz) => {
                                    if let Err(e) = gz.finish() {
                                        warn!("[Member {}] Error finishing evicted GZ stream for prefix {}: {}", member_id.0, evict_prefix.0, e);
                                    }
                                },
                                Err(e) => {
                                    warn!("[Member {}] Error getting inner GZ writer for prefix {} during eviction: {}", member_id.0, evict_prefix.0, e);
                                }
                            }
                        } else {
                            warn!("[Member {}] LRU contained prefix {} but it wasn't in the writers map.", member_id.0, evict_prefix.0);
                        }
                    } else {
                        error!("[Member {}] LRU queue empty while trying to evict final writer. Max open: {}", member_id.0, max_final_files_per_member);
                        break;
                    }
                }

                let prefix_dir = final_member_dir.join(&prefix.0);
                fs::create_dir_all(&prefix_dir)
                    .with_context(|| format!("[Member {}] Failed to create final prefix directory: {}", member_id.0, prefix_dir.display()))?;

                let final_file_path = prefix_dir.join("data.jsonl.gz");
                let file_exists = final_file_path.exists();
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(&final_file_path)
                    .with_context(|| format!("[Member {}] Failed to open/create final file: {}", member_id.0, final_file_path.display()))?;

                debug!("[Member {}] Opened final file{} {}", member_id.0, if file_exists { " (existing)" } else { "" }, final_file_path.display());
                let gz_encoder = GzEncoder::new(file, Compression::default());
                let writer = BufWriter::new(gz_encoder);

                writers.insert(prefix.clone(), writer);
                lru.push_front(prefix.clone());
                final_files_opened_count += 1;

            } else {
                if let Some(pos) = lru.iter().position(|p| p == &prefix) {
                    let p = lru.remove(pos).unwrap();
                    lru.push_front(p);
                } else {
                    warn!("[Member {}] Prefix {} writer exists but wasn't in LRU queue?", member_id.0, prefix.0);
                    lru.push_front(prefix.clone());
                }
            }

            let writer = writers.get_mut(&prefix).unwrap();
            writer.write_all(line_content.as_bytes())?;
            writer.write_all(b"\n")?;
            lines_written_count += 1;
            unique_prefixes_in_member.insert(prefix);

        }
    }

     debug!("[Member {}] Finishing and closing {} final writers...", member_id.0, writers.len());
     let mut finish_errors: Vec<String> = Vec::new();

     for (prefix, mut writer) in writers.into_iter() {
         if let Err(e) = writer.flush() {
             let msg = format!("[Member {}] Failed to flush final writer buffer for prefix {}: {}", member_id.0, prefix.0, e);
             error!("{}", msg);
             finish_errors.push(msg);
         }
         match writer.into_inner() {
             Ok(gz) => {
                 if let Err(e) = gz.finish() {
                     let msg = format!("[Member {}] Failed to finish GZ stream for prefix {}: {}", member_id.0, prefix.0, e);
                     error!("{}", msg);
                     finish_errors.push(msg);
                 } else {
                    debug!("[Member {}] Successfully finished GZ stream for prefix {}.", member_id.0, prefix.0);
                 }
             },
             Err(e) => {
                 let msg = format!("[Member {}] Failed to get inner GZ writer for prefix {} during final close: {}", member_id.0, prefix.0, e);
                 error!("{}", msg);
                 finish_errors.push(msg);
             }
         }
     }

    global_stats.pass2_members_processed.fetch_add(1, Ordering::Relaxed);
    global_stats.pass2_lines_read.fetch_add(lines_read_count, Ordering::Relaxed);
    global_stats.pass2_lines_written.fetch_add(lines_written_count, Ordering::Relaxed);
    global_stats.pass2_json_parse_errors.fetch_add(parse_error_count, Ordering::Relaxed);
    global_stats.pass2_prefix_missing.fetch_add(prefix_missing_count, Ordering::Relaxed);
    global_stats.pass2_lines_skipped_by_filter.fetch_add(skipped_by_filter_count, Ordering::Relaxed);
    global_stats.pass2_final_files_opened.fetch_add(final_files_opened_count, Ordering::Relaxed);
    {
        let mut prefixes_guard = global_stats.pass2_unique_prefixes_total.lock().unwrap();
        for prefix in unique_prefixes_in_member {
            prefixes_guard.insert((member_id.clone(), prefix));
        }
    }

    debug!("[Member {}] Removing intermediate directory: {}", member_id.0, member_dir_path.display());
    if let Err(e) = fs::remove_dir_all(&member_dir_path) {
        error!("[Member {}] CRITICAL: Failed to remove intermediate directory {}: {}. Manual cleanup required.", member_id.0, member_dir_path.display(), e);
    }

    let duration = start_time.elapsed();
    let prefixes_written_count = lru.len();

    info!(
        "[Member {}] Finished Pass 2 processing in {}. Lines Read: {}, Lines Written: {}, Lines Skipped: {}, Prefixes Written: {}, Files Opened: {}, Errors: {} parse, {} missing prefix.",
        member_id.0,
        format_elapsed(duration),
        lines_read_count,
        lines_written_count,
        skipped_by_filter_count,
        prefixes_written_count,
        final_files_opened_count,
        parse_error_count,
        prefix_missing_count
    );

    if !finish_errors.is_empty() {
        Err(anyhow::anyhow!("[Member {}] Errors occurred during final writer finish:\n - {}", member_id.0, finish_errors.join("\n - ")))
    } else {
        Ok(())
    }
}


fn main() -> Result<()> {
    let main_start_time = Instant::now();

    let cli = Cli::parse();

    let log_level = match cli.log_level.to_uppercase().as_str() {
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
        .with_timestamp_format(format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
        .init()?;

    info!("Starting Crossref Reorganizer v1.1.1");
    memory_usage::log_memory_usage("initial");

    let temp_dir_path = cli.temp_dir.map(PathBuf::from).unwrap_or_else(|| {
        let output_path = PathBuf::from(&cli.output_dir);
        let temp_path = output_path.join("_cr_temp");
        info!("Temporary directory not specified, using: {}", temp_path.display());
        temp_path
    });

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
        .build_global() {
        error!("Failed to build global thread pool: {}. Proceeding with default.", e);
    }

    let filter_config: Option<FilterConfig> = match &cli.filter_file {
        Some(filter_path) => {
            info!("Loading filter configuration from: {}", filter_path);
            let filter_content = fs::read_to_string(filter_path)
                .with_context(|| format!("Failed to read filter file: {}", filter_path))?;
            let parsed_filters: FilterFileConfig = serde_json::from_str(&filter_content)
                .with_context(|| format!("Failed to parse filter file as JSON: {}", filter_path))?;

            let mut runtime_filters = HashMap::new();
            for (member_str, prefixes_opt_vec) in parsed_filters {
                let member_id = MemberId(member_str.clone());
                let pattern1 = format!("\"member\": \"{}\"", member_str);
                let pattern2 = format!("\"member\": {}", member_str);
                let pattern3 = format!("\"member\": {}", member_str); // Keep this for flexibility, maybe spaces etc.
                let byte_patterns = vec![pattern1.into_bytes(), pattern2.into_bytes(), pattern3.into_bytes()];

                let prefixes_opt_set = prefixes_opt_vec.map(|vec| {
                    vec.into_iter().map(DoiPrefix).collect::<HashSet<DoiPrefix>>()
                });

                runtime_filters.insert(member_id, (prefixes_opt_set, byte_patterns));
            }
            info!("Successfully loaded filters and generated byte patterns for {} members.", runtime_filters.len());
            Some(Arc::new(runtime_filters))
        }
        None => {
            info!("No filter file specified. Processing all members and prefixes.");
            None
        }
    };

    info!("Searching for input files in: {}", cli.input);
    let files = find_jsonl_gz_files(&cli.input)?;
    if files.is_empty() {
        warn!("No .jsonl.gz files found in the specified directory. Exiting.");
        return Ok(());
    }
    info!("Found {} input files.", files.len());

    info!("Output directory: {}", cli.output_dir);
    info!("Temporary directory: {}", temp_dir_path.display());
    if let Some(path) = &cli.filter_file { info!("Filter file: {}", path); } else { info!("Filter file: None"); }
    info!("Max intermediate files (Pass 1): {}", cli.max_intermediate_files);
    info!("Max final files per member (Pass 2): {}", cli.max_final_files_per_member);
    info!("Statistics logging interval: {} seconds.", cli.stats_interval);

    let stats = Arc::new(Stats::new());

    let stats_thread_running = Arc::new(Mutex::new(true));
    let stats_interval_duration = Duration::from_secs(cli.stats_interval);
    let stats_clone_for_thread = Arc::clone(&stats);
    let stats_thread_running_clone = Arc::clone(&stats_thread_running);
    let stats_thread = std::thread::spawn(move || {
        info!("Stats logging thread started.");
        let mut last_log_time = Instant::now();
        loop {
            if let Ok(running) = stats_thread_running_clone.try_lock() {
                if !*running { info!("Stats thread received stop signal."); break; }
            } else { std::thread::sleep(Duration::from_millis(100)); continue; }

            std::thread::sleep(Duration::from_millis(500));

            if last_log_time.elapsed() >= stats_interval_duration {
                memory_usage::log_memory_usage("periodic check");
                stats_clone_for_thread.log_current_stats("Periodic");
                last_log_time = Instant::now();
            }
        }
        info!("Stats logging thread finished.");
    });

    info!("--- Starting Pass 1: Distributing records to temporary member files ---");
    let pass1_start_time = Instant::now();
    let intermediate_manager = Arc::new(IntermediateWriterManager::new(
        temp_dir_path.clone(),
        cli.max_intermediate_files
    )?);

    let progress_bar_pass1 = ProgressBar::new(files.len() as u64);
    // Restore the progress bar style definition
    progress_bar_pass1.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta} @ {per_sec}) {msg}")
            .expect("Failed to create progress bar template")
            .progress_chars("=> "),
    );
    progress_bar_pass1.set_message("Pass 1: Starting...");

    let all_byte_patterns: Option<Vec<Vec<u8>>> = filter_config.as_ref().map(|fc| {
        fc.values().flat_map(|(_, patterns)| patterns.iter().cloned()).collect()
    });

    let stats_pass1 = Arc::clone(&stats);
    let pass1_results: Vec<Result<_, String>> = files
        .par_iter()
        .map(|filepath| {
            let pb_clone = progress_bar_pass1.clone();
            let manager_clone = Arc::clone(&intermediate_manager);
            let stats_clone = Arc::clone(&stats_pass1);
            let filter_clone = filter_config.clone();
            let patterns_clone = all_byte_patterns.clone();

            let mut file_lines_read = 0u64;
            let mut file_lines_written = 0u64;
            let mut file_parse_errors = 0u64;
            let mut file_missing_member = 0u64;
            let mut file_skipped_by_filter = 0u64;
            let mut file_pre_filtered = 0u64;
            let mut members_found_in_file = HashSet::new();

             let file = match File::open(filepath) {
                 Ok(f) => f,
                 Err(e) => return Err(format!("Failed to open {}: {}", filepath.display(), e)),
             };
             let decoder = GzDecoder::new(file);
             let mut reader = BufReader::new(decoder);
             let mut byte_buffer = Vec::with_capacity(1024);

             loop {
                 byte_buffer.clear();
                 match reader.read_until(b'\n', &mut byte_buffer) {
                     Ok(0) => break,
                     Ok(_) => {
                         file_lines_read += 1;
                         let mut proceed_to_parse = true;
                         if let Some(ref patterns) = patterns_clone {
                             if !byte_buffer.is_empty() && !contains_any_pattern(&byte_buffer, patterns) {
                                 file_pre_filtered += 1;
                                 proceed_to_parse = false;
                             }
                         }

                         if proceed_to_parse && !byte_buffer.is_empty() {
                             match std::str::from_utf8(&byte_buffer) {
                                 Ok(line_str) => {
                                     if line_str.trim().is_empty() { continue; }
                                     match serde_json::from_str::<Pass1Record>(line_str) {
                                         Ok(record) => {
                                             match extract_member_id_from_value(&record.member) {
                                                 Some(member_id) => {
                                                     let should_process = match &filter_clone {
                                                          Some(ref filters) => filters.contains_key(&member_id),
                                                          None => true,
                                                      };
                                                     if should_process {
                                                         members_found_in_file.insert(member_id.clone());
                                                         match manager_clone.write_line(&member_id, line_str) {
                                                             Ok(_) => file_lines_written += 1,
                                                             Err(e) => error!("Failed to write line for member {} from {}: {}", member_id.0, filepath.display(), e),
                                                         }
                                                     } else {
                                                         file_skipped_by_filter += 1;
                                                     }
                                                 },
                                                 None => file_missing_member += 1,
                                             }
                                         },
                                         Err(e) => {
                                             file_parse_errors += 1;
                                             if file_parse_errors % 10000 == 1 { warn!("JSON parse error in {}: {} (line starts: {}...)", filepath.display(), e, line_str.chars().take(50).collect::<String>()); }
                                         }
                                     }
                                 },
                                 Err(e) => {
                                     file_parse_errors += 1;
                                     if file_parse_errors % 10000 == 1 { warn!("UTF-8 decode error in {}: {} (bytes: {:?})", filepath.display(), e, byte_buffer.iter().take(50).collect::<Vec<_>>()); }
                                 }
                             }
                         }
                     }
                     Err(e) => {
                         warn!("Error reading line/bytes from {}: {}", filepath.display(), e);
                         file_parse_errors += 1;
                         continue;
                     }
                 }
             }

            stats_clone.pass1_lines_read.fetch_add(file_lines_read, Ordering::Relaxed);
            stats_clone.pass1_lines_written.fetch_add(file_lines_written, Ordering::Relaxed);
            stats_clone.pass1_json_parse_errors.fetch_add(file_parse_errors, Ordering::Relaxed);
            stats_clone.pass1_member_id_missing.fetch_add(file_missing_member, Ordering::Relaxed);
            stats_clone.pass1_lines_skipped_by_filter.fetch_add(file_skipped_by_filter, Ordering::Relaxed);
            stats_clone.pass1_lines_pre_filtered.fetch_add(file_pre_filtered, Ordering::Relaxed);
            if !members_found_in_file.is_empty() {
                 let mut members_guard = stats_clone.pass1_unique_members_found.lock().unwrap();
                 members_guard.extend(members_found_in_file);
            }

            pb_clone.inc(1);
            let file_name_msg = filepath.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
            pb_clone.set_message(format!("Pass 1: {}", file_name_msg));

            Ok(())
        })
        .collect();

    intermediate_manager.flush_all()?;
    progress_bar_pass1.finish_with_message("Pass 1: Finished distribution.");

    let pass1_duration = pass1_start_time.elapsed();
    let pass1_files_opened_final = intermediate_manager.get_files_opened_count();
    stats.pass1_intermediate_files_opened.store(pass1_files_opened_final, Ordering::Relaxed);
    stats.log_current_stats("Pass 1");

    let final_pass1_errors = pass1_results.iter().filter(|r| r.is_err()).count();
    info!("--- Pass 1 Summary ---");
    info!("Duration: {}", format_elapsed(pass1_duration));
    info!("Input files processed: {} ({} errors)", files.len(), final_pass1_errors);
    if final_pass1_errors > 0 {
        warn!("{} input files encountered errors during Pass 1.", final_pass1_errors);
        for result in pass1_results.iter().filter_map(|r| r.as_ref().err()) { error!("  - {}", result); }
    }

    info!("--- Starting Pass 2: Consolidating temporary files and splitting by prefix ---");
    let pass2_start_time = Instant::now();
    let filter_config_pass2 = filter_config.clone();
    let mut final_pass2_errors = 0;
    let mut member_dirs_processed_count = 0;

    let member_dirs = match fs::read_dir(&temp_dir_path) {
        Ok(reader) => reader.filter_map(Result::ok)
                           .filter(|entry| entry.file_type().map_or(false, |ft| ft.is_dir()))
                           .map(|entry| entry.path())
                           .collect::<Vec<_>>(),
        Err(e) => {
            error!("Failed to read temporary directory {}: {}", temp_dir_path.display(), e);
            if let Err(cleanup_err) = fs::remove_dir_all(&temp_dir_path) { error!("Failed to cleanup temporary directory {}: {}", temp_dir_path.display(), cleanup_err); }
            info!("Signaling stats thread to stop...");
            if let Ok(mut running_guard) = stats_thread_running.lock() { *running_guard = false; } else { error!("Failed to lock stats thread running flag to signal stop."); }
            info!("Waiting for stats thread to finish..."); if let Err(e) = stats_thread.join() { error!("Error joining stats thread: {:?}", e); } else { info!("Stats thread joined successfully."); }
            return Err(e.into());
        }
    };

    member_dirs_processed_count = member_dirs.len();

    if member_dirs.is_empty() {
        info!("No intermediate member directories found in {}. Nothing to process in Pass 2.", temp_dir_path.display());
        if temp_dir_path.exists() {
             if let Err(cleanup_err) = fs::remove_dir_all(&temp_dir_path) { error!("Failed to cleanup temporary directory {}: {}", temp_dir_path.display(), cleanup_err); }
        }
    } else {
        info!("Found {} member directories to process in Pass 2.", member_dirs_processed_count);
        let progress_bar_pass2 = ProgressBar::new(member_dirs_processed_count as u64);
        progress_bar_pass2.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
                .expect("Failed to create progress bar template")
                .progress_chars("=> "),
        );
        progress_bar_pass2.set_message("Pass 2: Starting...");

        let stats_pass2 = Arc::clone(&stats);
        let pass2_results: Vec<Result<(), String>> = member_dirs
            .par_iter()
            .map(|member_dir_path| {
                let pb_clone = progress_bar_pass2.clone();
                let output_base = PathBuf::from(&cli.output_dir);
                let stats_clone = Arc::clone(&stats_pass2);
                let filter_clone = filter_config_pass2.clone();
                let member_id_str = member_dir_path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or("?".to_string());
                pb_clone.set_message(format!("Pass 2: Member {}", member_id_str));
                let result = process_member_directory( member_dir_path.clone(), output_base, cli.max_final_files_per_member, stats_clone, filter_clone );
                pb_clone.inc(1);
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => { error!("Error processing member directory {}: {}", member_dir_path.display(), e); Err(format!("Failed {}: {}", member_dir_path.display(), e)) }
                }
            })
            .collect();

        progress_bar_pass2.finish_with_message("Pass 2: Finished consolidation.");
        let pass2_duration = pass2_start_time.elapsed();
        stats.log_current_stats("Pass 2");

        final_pass2_errors = pass2_results.iter().filter(|r| r.is_err()).count();
        info!("--- Pass 2 Summary ---");
        info!("Duration: {}", format_elapsed(pass2_duration));
        info!("Member directories processed: {} ({} errors)", member_dirs_processed_count, final_pass2_errors);
        if final_pass2_errors > 0 {
            warn!("{} member directories encountered errors during Pass 2.", final_pass2_errors);
            for result in pass2_results.iter().filter_map(|r| r.as_ref().err()) { error!("  - {}", result); }
            warn!("Note: Intermediate directories for failed members might not have been cleaned up in {}", temp_dir_path.display());
        }
    }

    info!("Attempting final cleanup of temporary directory: {}", temp_dir_path.display());
    match fs::remove_dir_all(&temp_dir_path) {
        Ok(_) => info!("Temporary directory successfully removed."),
        Err(e) => {
            if temp_dir_path.exists() { warn!("Failed to remove temporary directory (it might contain failed member data): {} - {}", temp_dir_path.display(), e); }
            else { info!("Temporary directory already removed (likely by last successful member process or was empty)."); }
        }
    }

    info!("Signaling stats thread to stop...");
    if let Ok(mut running_guard) = stats_thread_running.lock() { *running_guard = false; } else { error!("Failed to lock stats thread running flag to signal stop."); }
    info!("Waiting for stats thread to finish..."); if let Err(e) = stats_thread.join() { error!("Error joining stats thread: {:?}", e); } else { info!("Stats thread joined successfully."); }

    info!("-------------------- FINAL SUMMARY --------------------");
    let total_runtime = main_start_time.elapsed();
    info!("Total execution time: {}", format_elapsed(total_runtime));
    info!("Input files found: {}", files.len());
    if let Some(f) = &cli.filter_file { info!("Filter file used: {}", f); }
    info!("Pass 1 file processing errors: {}", final_pass1_errors);
    info!("Pass 2 member processing errors: {}", final_pass2_errors);

    stats.log_current_stats("Final");
    memory_usage::log_memory_usage("final");
    info!("Reorganization process finished.");
    info!("-------------------------------------------------------");

    if final_pass1_errors > 0 || final_pass2_errors > 0 {
        error!("Processing finished with errors.");
        Err(anyhow::anyhow!("Processing finished with errors."))
    } else {
        info!("Processing finished successfully.");
        Ok(())
    }
}
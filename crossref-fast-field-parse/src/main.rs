use anyhow::{Context, Result};
use clap::Parser;
use csv::Writer;
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::DashMap;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use log::{debug, error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde_json::Value;
use simple_logger::SimpleLogger;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use time::macros::format_description;
#[cfg(target_os = "linux")]
use std::fs::read_to_string;
#[cfg(target_os = "windows")]
use std::process::Command as WinCommand;

#[derive(Parser)]
#[command(name = "Crossref Data File Fast Field Parser")]
#[command(about = "Efficiently extracts field data from the Crossref data file in its compressed JSONL.gz format")]
#[command(version = "1.0.")]
struct Cli {
    #[arg(short, long, help = "Directory containing JSONL.gz files", required = true)]
    input: String,

    #[arg(short, long, default_value = "field_data.csv", help = "Output CSV file or directory")]
    output: String,

    #[arg(short, long, default_value = "INFO", help = "Logging level (DEBUG, INFO, WARN, ERROR)")]
    log_level: String,

    #[arg(short, long, default_value = "0", help = "Number of threads to use (0 for auto)")]
    threads: usize,

    #[arg(short, long, default_value = "10000", help = "Target number of records per batch sent to writer")]
    batch_size: usize,

    #[arg(short, long, default_value = "60", help = "Interval in seconds to log statistics")]
    stats_interval: u64,

    #[arg(short = 'g', long, help = "Organize output by member ID")]
    organize: bool,

    #[arg(long, help = "Filter by member ID")]
    member: Option<String>,

    #[arg(long, help = "Filter by DOI prefix")]
    doi_prefix: Option<String>,

    #[arg(long, default_value = "100", help = "Maximum number of open files when using --organize")]
    max_open_files: usize,

    #[arg(short, long, help = "Comma-separated list of fields to extract (e.g., 'author.family,title,ISSN')")]
    fields: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Doi(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MemberId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DoiPrefix(String);

#[derive(Debug, Clone)]
struct FieldData {
    doi: Doi,
    field_name: String,
    subfield_path: String,
    value: String,
    member_id: MemberId,
    doi_prefix: DoiPrefix,
}

impl Default for FieldData {
    fn default() -> Self {
        Self {
            doi: Doi(String::new()),
            field_name: String::new(),
            subfield_path: String::new(),
            value: String::new(),
            member_id: MemberId(String::new()),
            doi_prefix: DoiPrefix(String::new()),
        }
    }
}

#[derive(Debug, Default)]
struct FileStats {
    unique_dois: HashSet<Doi>,
    field_counts: HashMap<String, usize>,
    member_counts: HashMap<MemberId, usize>,
    prefix_counts: HashMap<DoiPrefix, usize>,
    total_fields_extracted: usize,
}

enum FileBatchResult {
    Success(Vec<FieldData>, FileStats),
    Error(PathBuf, anyhow::Error),
}

struct IncrementalStats {
    total_field_records: AtomicUsize,
    processed_files_ok: AtomicUsize,
    processed_files_error: AtomicUsize,

    unique_records: DashMap<String, bool>,
    members: DashMap<MemberId, AtomicUsize>,
    prefixes: DashMap<DoiPrefix, AtomicUsize>,
    unique_fields: DashMap<String, AtomicUsize>,
}

impl IncrementalStats {
    fn new() -> Self {
        Self {
            total_field_records: AtomicUsize::new(0),
            processed_files_ok: AtomicUsize::new(0),
            processed_files_error: AtomicUsize::new(0),
            unique_records: DashMap::new(),
            members: DashMap::new(),
            prefixes: DashMap::new(),
            unique_fields: DashMap::new(),
        }
    }

    fn aggregate_file_stats(&self, file_stats: FileStats) {
        self.processed_files_ok.fetch_add(1, Ordering::Relaxed);
        self.total_field_records.fetch_add(file_stats.total_fields_extracted, Ordering::Relaxed);

        for doi in file_stats.unique_dois {
            self.unique_records.insert(doi.0, true);
        }

        for (field_name, count) in file_stats.field_counts {
             self.unique_fields.entry(field_name)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(count, Ordering::Relaxed);
        }

        for (member_id, count) in file_stats.member_counts {
             self.members.entry(member_id)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(count, Ordering::Relaxed);
        }

         for (prefix, count) in file_stats.prefix_counts {
             self.prefixes.entry(prefix)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(count, Ordering::Relaxed);
        }
    }

    fn increment_error_files(&self) {
        self.processed_files_error.fetch_add(1, Ordering::Relaxed);
    }


    fn log_current_stats(&self) {
        let files_ok = self.processed_files_ok.load(Ordering::Relaxed);
        let files_err = self.processed_files_error.load(Ordering::Relaxed);
        let total_fields = self.total_field_records.load(Ordering::Relaxed);
        let unique_dois = self.unique_records.len();
        let unique_fields_count = self.unique_fields.len();
        let unique_members = self.members.len();
        let unique_prefixes = self.prefixes.len();

        info!("Current Statistics:");
        info!("  Files processed: {} OK, {} Errors", files_ok, files_err);
        info!("  Total field records extracted: {}", total_fields);
        info!("  Unique DOIs encountered: {}", unique_dois);
        info!("  Unique fields encountered: {}", unique_fields_count);

        let mut field_snapshot: Vec<(String, usize)> = self.unique_fields
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();

        field_snapshot.sort_by_key(|&(_, count)| std::cmp::Reverse(count));

        info!("  Field breakdown (Top 10):");
        for (field, count) in field_snapshot.iter().take(10) {
            info!("    {}: {} records", field, count);
        }
        if field_snapshot.len() > 10 {
            info!("    ... ({} more fields)", field_snapshot.len() - 10);
        }

        info!("  Unique members: {}", unique_members);
        info!("  Unique DOI prefixes: {}", unique_prefixes);
    }

    fn get_final_stats(&self) -> FinalStats {
         let mut final_fields = HashMap::new();
         for entry in self.unique_fields.iter() {
            final_fields.insert(entry.key().clone(), entry.value().load(Ordering::Relaxed));
         }

         let mut final_members = HashMap::new();
          for entry in self.members.iter() {
            final_members.insert(entry.key().clone(), entry.value().load(Ordering::Relaxed));
         }

         let mut final_prefixes = HashMap::new();
          for entry in self.prefixes.iter() {
             final_prefixes.insert(entry.key().clone(), entry.value().load(Ordering::Relaxed));
          }

         FinalStats {
             total_field_records: self.total_field_records.load(Ordering::Relaxed),
             processed_files_ok: self.processed_files_ok.load(Ordering::Relaxed),
             processed_files_error: self.processed_files_error.load(Ordering::Relaxed),
             unique_dois: self.unique_records.len(),
             unique_members: final_members,
             unique_prefixes: final_prefixes,
             unique_fields: final_fields,
         }
    }
}

struct FinalStats {
    total_field_records: usize,
    processed_files_ok: usize,
    processed_files_error: usize,
    unique_dois: usize,
    unique_members: HashMap<MemberId, usize>,
    unique_prefixes: HashMap<DoiPrefix, usize>,
    unique_fields: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum FieldType {
    Array,
    Object,
    Value,
}

lazy_static! {
    static ref SCHEMA_STRUCTURE: HashMap<String, FieldType> = {
        let mut schema = HashMap::new();

        schema.insert("DOI".to_string(), FieldType::Value);
        schema.insert("ISSN".to_string(), FieldType::Array);
        schema.insert("URL".to_string(), FieldType::Value);
        schema.insert("alternative-id".to_string(), FieldType::Array);
        schema.insert("author".to_string(), FieldType::Array);
        schema.insert("author.affiliation".to_string(), FieldType::Array);
        schema.insert("author.affiliation.name".to_string(), FieldType::Value);
        schema.insert("author.affiliation.place".to_string(), FieldType::Array);
        schema.insert("author.affiliation.id".to_string(), FieldType::Array);
        schema.insert("author.affiliation.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("author.affiliation.id.id".to_string(), FieldType::Value);
        schema.insert("author.affiliation.id.id-type".to_string(), FieldType::Value);
        schema.insert("author.affiliation.department".to_string(), FieldType::Array);
        schema.insert("author.affiliation.acronym".to_string(), FieldType::Array);
        schema.insert("author.family".to_string(), FieldType::Value);
        schema.insert("author.given".to_string(), FieldType::Value);
        schema.insert("author.sequence".to_string(), FieldType::Value);
        schema.insert("author.name".to_string(), FieldType::Value);
        schema.insert("author.suffix".to_string(), FieldType::Value);
        schema.insert("author.ORCID".to_string(), FieldType::Value);
        schema.insert("author.authenticated-orcid".to_string(), FieldType::Value);
        schema.insert("container-title".to_string(), FieldType::Array);
        schema.insert("content-domain".to_string(), FieldType::Object);
        schema.insert("content-domain.crossmark-restriction".to_string(), FieldType::Value);
        schema.insert("content-domain.domain".to_string(), FieldType::Array);
        schema.insert("created".to_string(), FieldType::Object);
        schema.insert("created.date-parts".to_string(), FieldType::Array);
        schema.insert("created.date-time".to_string(), FieldType::Value);
        schema.insert("created.timestamp".to_string(), FieldType::Value);
        schema.insert("deposited".to_string(), FieldType::Object);
        schema.insert("deposited.date-parts".to_string(), FieldType::Array);
        schema.insert("deposited.date-time".to_string(), FieldType::Value);
        schema.insert("deposited.timestamp".to_string(), FieldType::Value);
        schema.insert("indexed".to_string(), FieldType::Object);
        schema.insert("indexed.date-parts".to_string(), FieldType::Array);
        schema.insert("indexed.date-time".to_string(), FieldType::Value);
        schema.insert("indexed.timestamp".to_string(), FieldType::Value);
        schema.insert("indexed.version".to_string(), FieldType::Value);
        schema.insert("is-referenced-by-count".to_string(), FieldType::Value);
        schema.insert("issn-type".to_string(), FieldType::Array);
        schema.insert("issn-type.type".to_string(), FieldType::Value);
        schema.insert("issn-type.value".to_string(), FieldType::Value);
        schema.insert("issue".to_string(), FieldType::Value);
        schema.insert("issued".to_string(), FieldType::Object);
        schema.insert("issued.date-parts".to_string(), FieldType::Array);
        schema.insert("journal-issue".to_string(), FieldType::Object);
        schema.insert("journal-issue.issue".to_string(), FieldType::Value);
        schema.insert("journal-issue.published-print".to_string(), FieldType::Object);
        schema.insert("journal-issue.published-print.date-parts".to_string(), FieldType::Array);
        schema.insert("journal-issue.published-online".to_string(), FieldType::Object);
        schema.insert("journal-issue.published-online.date-parts".to_string(), FieldType::Array);
        schema.insert("language".to_string(), FieldType::Value);
        schema.insert("license".to_string(), FieldType::Array);
        schema.insert("license.URL".to_string(), FieldType::Value);
        schema.insert("license.content-version".to_string(), FieldType::Value);
        schema.insert("license.delay-in-days".to_string(), FieldType::Value);
        schema.insert("license.start".to_string(), FieldType::Object);
        schema.insert("license.start.date-parts".to_string(), FieldType::Array);
        schema.insert("license.start.date-time".to_string(), FieldType::Value);
        schema.insert("license.start.timestamp".to_string(), FieldType::Value);
        schema.insert("link".to_string(), FieldType::Array);
        schema.insert("link.URL".to_string(), FieldType::Value);
        schema.insert("link.content-type".to_string(), FieldType::Value);
        schema.insert("link.content-version".to_string(), FieldType::Value);
        schema.insert("link.intended-application".to_string(), FieldType::Value);
        schema.insert("member".to_string(), FieldType::Value);
        schema.insert("page".to_string(), FieldType::Value);
        schema.insert("prefix".to_string(), FieldType::Value);
        schema.insert("published".to_string(), FieldType::Object);
        schema.insert("published.date-parts".to_string(), FieldType::Array);
        schema.insert("published-print".to_string(), FieldType::Object);
        schema.insert("published-print.date-parts".to_string(), FieldType::Array);
        schema.insert("publisher".to_string(), FieldType::Value);
        schema.insert("reference".to_string(), FieldType::Array);
        schema.insert("reference.article-title".to_string(), FieldType::Value);
        schema.insert("reference.author".to_string(), FieldType::Value);
        schema.insert("reference.first-page".to_string(), FieldType::Value);
        schema.insert("reference.journal-title".to_string(), FieldType::Value);
        schema.insert("reference.key".to_string(), FieldType::Value);
        schema.insert("reference.volume".to_string(), FieldType::Value);
        schema.insert("reference.year".to_string(), FieldType::Value);
        schema.insert("reference.DOI".to_string(), FieldType::Value);
        schema.insert("reference.doi-asserted-by".to_string(), FieldType::Value);
        schema.insert("reference.unstructured".to_string(), FieldType::Value);
        schema.insert("reference.issue".to_string(), FieldType::Value);
        schema.insert("reference.series-title".to_string(), FieldType::Value);
        schema.insert("reference.volume-title".to_string(), FieldType::Value);
        schema.insert("reference.edition".to_string(), FieldType::Value);
        schema.insert("reference.ISSN".to_string(), FieldType::Value);
        schema.insert("reference.issn-type".to_string(), FieldType::Value);
        schema.insert("reference.ISBN".to_string(), FieldType::Value);
        schema.insert("reference.isbn-type".to_string(), FieldType::Value);
        schema.insert("reference.component".to_string(), FieldType::Value);
        schema.insert("reference.standards-body".to_string(), FieldType::Value);
        schema.insert("reference.standard-designator".to_string(), FieldType::Value);
        schema.insert("reference-count".to_string(), FieldType::Value);
        schema.insert("references-count".to_string(), FieldType::Value);
        schema.insert("resource".to_string(), FieldType::Object);
        schema.insert("resource.primary".to_string(), FieldType::Object);
        schema.insert("resource.primary.URL".to_string(), FieldType::Value);
        schema.insert("resource.secondary".to_string(), FieldType::Array);
        schema.insert("resource.secondary.URL".to_string(), FieldType::Value);
        schema.insert("resource.secondary.label".to_string(), FieldType::Value);
        schema.insert("score".to_string(), FieldType::Value);
        schema.insert("short-container-title".to_string(), FieldType::Array);
        schema.insert("source".to_string(), FieldType::Value);
        schema.insert("title".to_string(), FieldType::Array);
        schema.insert("volume".to_string(), FieldType::Value);
        schema.insert("special_numbering".to_string(), FieldType::Value);
        schema.insert("published-online".to_string(), FieldType::Object);
        schema.insert("published-online.date-parts".to_string(), FieldType::Array);
        schema.insert("abstract".to_string(), FieldType::Value);
        schema.insert("article-number".to_string(), FieldType::Value);
        schema.insert("archive".to_string(), FieldType::Array);
        schema.insert("assertion".to_string(), FieldType::Array);
        schema.insert("assertion.group".to_string(), FieldType::Object);
        schema.insert("assertion.group.label".to_string(), FieldType::Value);
        schema.insert("assertion.group.name".to_string(), FieldType::Value);
        schema.insert("assertion.label".to_string(), FieldType::Value);
        schema.insert("assertion.name".to_string(), FieldType::Value);
        schema.insert("assertion.order".to_string(), FieldType::Value);
        schema.insert("assertion.value".to_string(), FieldType::Value);
        schema.insert("assertion.explanation".to_string(), FieldType::Object);
        schema.insert("assertion.explanation.URL".to_string(), FieldType::Value);
        schema.insert("assertion.URL".to_string(), FieldType::Value);
        schema.insert("update-policy".to_string(), FieldType::Value);
        schema.insert("subtitle".to_string(), FieldType::Array);
        schema.insert("updated-by".to_string(), FieldType::Array);
        schema.insert("updated-by.DOI".to_string(), FieldType::Value);
        schema.insert("updated-by.label".to_string(), FieldType::Value);
        schema.insert("updated-by.source".to_string(), FieldType::Value);
        schema.insert("updated-by.type".to_string(), FieldType::Value);
        schema.insert("updated-by.updated".to_string(), FieldType::Object);
        schema.insert("updated-by.updated.date-parts".to_string(), FieldType::Array);
        schema.insert("updated-by.updated.date-time".to_string(), FieldType::Value);
        schema.insert("updated-by.updated.timestamp".to_string(), FieldType::Value);
        schema.insert("updated-by.record-id".to_string(), FieldType::Value);
        schema.insert("relation".to_string(), FieldType::Object);
        schema.insert("relation.*".to_string(), FieldType::Array);
        schema.insert("relation.*.asserted-by".to_string(), FieldType::Value);
        schema.insert("relation.*.id".to_string(), FieldType::Value);
        schema.insert("relation.*.id-type".to_string(), FieldType::Value);
        schema.insert("funder".to_string(), FieldType::Array);
        schema.insert("funder.DOI".to_string(), FieldType::Value);
        schema.insert("funder.doi-asserted-by".to_string(), FieldType::Value);
        schema.insert("funder.id".to_string(), FieldType::Array);
        schema.insert("funder.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("funder.id.id".to_string(), FieldType::Value);
        schema.insert("funder.id.id-type".to_string(), FieldType::Value);
        schema.insert("funder.name".to_string(), FieldType::Value);
        schema.insert("funder.award".to_string(), FieldType::Array);
        schema.insert("update-to".to_string(), FieldType::Array);
        schema.insert("update-to.DOI".to_string(), FieldType::Value);
        schema.insert("update-to.label".to_string(), FieldType::Value);
        schema.insert("update-to.record-id".to_string(), FieldType::Value);
        schema.insert("update-to.source".to_string(), FieldType::Value);
        schema.insert("update-to.type".to_string(), FieldType::Value);
        schema.insert("update-to.updated".to_string(), FieldType::Object);
        schema.insert("update-to.updated.date-parts".to_string(), FieldType::Array);
        schema.insert("update-to.updated.date-time".to_string(), FieldType::Value);
        schema.insert("update-to.updated.timestamp".to_string(), FieldType::Value);
        schema.insert("published-other".to_string(), FieldType::Object);
        schema.insert("published-other.date-parts".to_string(), FieldType::Array);
        schema.insert("editor".to_string(), FieldType::Array);
        schema.insert("editor.affiliation".to_string(), FieldType::Array);
        schema.insert("editor.affiliation.name".to_string(), FieldType::Value);
        schema.insert("editor.affiliation.id".to_string(), FieldType::Array);
        schema.insert("editor.affiliation.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("editor.affiliation.id.id".to_string(), FieldType::Value);
        schema.insert("editor.affiliation.id.id-type".to_string(), FieldType::Value);
        schema.insert("editor.affiliation.place".to_string(), FieldType::Array);
        schema.insert("editor.affiliation.acronym".to_string(), FieldType::Array);
        schema.insert("editor.affiliation.department".to_string(), FieldType::Array);
        schema.insert("editor.family".to_string(), FieldType::Value);
        schema.insert("editor.given".to_string(), FieldType::Value);
        schema.insert("editor.sequence".to_string(), FieldType::Value);
        schema.insert("editor.ORCID".to_string(), FieldType::Value);
        schema.insert("editor.authenticated-orcid".to_string(), FieldType::Value);
        schema.insert("editor.name".to_string(), FieldType::Value);
        schema.insert("editor.suffix".to_string(), FieldType::Value);
        schema.insert("aliases".to_string(), FieldType::Array);
        schema.insert("original-title".to_string(), FieldType::Array);
        schema.insert("ISBN".to_string(), FieldType::Array);
        schema.insert("isbn-type".to_string(), FieldType::Array);
        schema.insert("isbn-type.type".to_string(), FieldType::Value);
        schema.insert("isbn-type.value".to_string(), FieldType::Value);
        schema.insert("publisher-location".to_string(), FieldType::Value);
        schema.insert("description".to_string(), FieldType::Value);
        schema.insert("event".to_string(), FieldType::Object);
        schema.insert("event.location".to_string(), FieldType::Value);
        schema.insert("event.name".to_string(), FieldType::Value);
        schema.insert("event.end".to_string(), FieldType::Object);
        schema.insert("event.end.date-parts".to_string(), FieldType::Array);
        schema.insert("event.start".to_string(), FieldType::Object);
        schema.insert("event.start.date-parts".to_string(), FieldType::Array);
        schema.insert("event.acronym".to_string(), FieldType::Value);
        schema.insert("event.sponsor".to_string(), FieldType::Array);
        schema.insert("event.number".to_string(), FieldType::Value);
        schema.insert("event.theme".to_string(), FieldType::Value);
        schema.insert("accepted".to_string(), FieldType::Object);
        schema.insert("accepted.date-parts".to_string(), FieldType::Array);
        schema.insert("short-title".to_string(), FieldType::Array);
        schema.insert("review".to_string(), FieldType::Object);
        schema.insert("review.competing-interest-statement".to_string(), FieldType::Value);
        schema.insert("review.recommendation".to_string(), FieldType::Value);
        schema.insert("review.revision-round".to_string(), FieldType::Value);
        schema.insert("review.stage".to_string(), FieldType::Value);
        schema.insert("review.type".to_string(), FieldType::Value);
        schema.insert("review.language".to_string(), FieldType::Value);
        schema.insert("review.running-number".to_string(), FieldType::Value);
        schema.insert("group-title".to_string(), FieldType::Value);
        schema.insert("institution".to_string(), FieldType::Array);
        schema.insert("institution.name".to_string(), FieldType::Value);
        schema.insert("institution.place".to_string(), FieldType::Array);
        schema.insert("institution.acronym".to_string(), FieldType::Array);
        schema.insert("institution.department".to_string(), FieldType::Array);
        schema.insert("institution.id".to_string(), FieldType::Array);
        schema.insert("institution.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("institution.id.id".to_string(), FieldType::Value);
        schema.insert("institution.id.id-type".to_string(), FieldType::Value);
        schema.insert("posted".to_string(), FieldType::Object);
        schema.insert("posted.date-parts".to_string(), FieldType::Array);
        schema.insert("subtype".to_string(), FieldType::Value);
        schema.insert("approved".to_string(), FieldType::Object);
        schema.insert("approved.date-parts".to_string(), FieldType::Array);
        schema.insert("standards-body".to_string(), FieldType::Object);
        schema.insert("standards-body.acronym".to_string(), FieldType::Value);
        schema.insert("standards-body.name".to_string(), FieldType::Value);
        schema.insert("content-created".to_string(), FieldType::Object);
        schema.insert("content-created.date-parts".to_string(), FieldType::Array);
        schema.insert("edition-number".to_string(), FieldType::Value);
        schema.insert("degree".to_string(), FieldType::Array);
        schema.insert("issue-title".to_string(), FieldType::Array);
        schema.insert("translator".to_string(), FieldType::Array);
        schema.insert("translator.affiliation".to_string(), FieldType::Array);
        schema.insert("translator.affiliation.name".to_string(), FieldType::Value);
        schema.insert("translator.affiliation.id".to_string(), FieldType::Array);
        schema.insert("translator.affiliation.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("translator.affiliation.id.id".to_string(), FieldType::Value);
        schema.insert("translator.affiliation.id.id-type".to_string(), FieldType::Value);
        schema.insert("translator.affiliation.place".to_string(), FieldType::Array);
        schema.insert("translator.family".to_string(), FieldType::Value);
        schema.insert("translator.given".to_string(), FieldType::Value);
        schema.insert("translator.sequence".to_string(), FieldType::Value);
        schema.insert("translator.name".to_string(), FieldType::Value);
        schema.insert("translator.ORCID".to_string(), FieldType::Value);
        schema.insert("translator.authenticated-orcid".to_string(), FieldType::Value);
        schema.insert("translator.suffix".to_string(), FieldType::Value);
        schema.insert("clinical-trial-number".to_string(), FieldType::Array);
        schema.insert("clinical-trial-number.clinical-trial-number".to_string(), FieldType::Value);
        schema.insert("clinical-trial-number.registry".to_string(), FieldType::Value);
        schema.insert("clinical-trial-number.type".to_string(), FieldType::Value);
        schema.insert("award".to_string(), FieldType::Value);
        schema.insert("award-start".to_string(), FieldType::Object);
        schema.insert("award-start.date-parts".to_string(), FieldType::Array);
        schema.insert("project".to_string(), FieldType::Array);
        schema.insert("project.award-end".to_string(), FieldType::Object);
        schema.insert("project.award-end.date-parts".to_string(), FieldType::Array);
        schema.insert("project.award-start".to_string(), FieldType::Object);
        schema.insert("project.award-start.date-parts".to_string(), FieldType::Array);
        schema.insert("project.funding".to_string(), FieldType::Array);
        schema.insert("project.funding.funder".to_string(), FieldType::Object);
        schema.insert("project.funding.funder.id".to_string(), FieldType::Array);
        schema.insert("project.funding.funder.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("project.funding.funder.id.id".to_string(), FieldType::Value);
        schema.insert("project.funding.funder.id.id-type".to_string(), FieldType::Value);
        schema.insert("project.funding.funder.name".to_string(), FieldType::Value);
        schema.insert("project.funding.type".to_string(), FieldType::Value);
        schema.insert("project.funding.scheme".to_string(), FieldType::Value);
        schema.insert("project.funding.award-amount".to_string(), FieldType::Object);
        schema.insert("project.funding.award-amount.amount".to_string(), FieldType::Value);
        schema.insert("project.funding.award-amount.currency".to_string(), FieldType::Value);
        schema.insert("project.funding.award-amount.percentage".to_string(), FieldType::Value);
        schema.insert("project.investigator".to_string(), FieldType::Array);
        schema.insert("project.investigator.affiliation".to_string(), FieldType::Array);
        schema.insert("project.investigator.affiliation.country".to_string(), FieldType::Value);
        schema.insert("project.investigator.affiliation.name".to_string(), FieldType::Value);
        schema.insert("project.investigator.affiliation.id".to_string(), FieldType::Array);
        schema.insert("project.investigator.affiliation.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("project.investigator.affiliation.id.id".to_string(), FieldType::Value);
        schema.insert("project.investigator.affiliation.id.id-type".to_string(), FieldType::Value);
        schema.insert("project.investigator.family".to_string(), FieldType::Value);
        schema.insert("project.investigator.given".to_string(), FieldType::Value);
        schema.insert("project.investigator.ORCID".to_string(), FieldType::Value);
        schema.insert("project.investigator.authenticated-orcid".to_string(), FieldType::Value);
        schema.insert("project.investigator.alternate-name".to_string(), FieldType::Array);
        schema.insert("project.investigator.role-start".to_string(), FieldType::Object);
        schema.insert("project.investigator.role-start.date-parts".to_string(), FieldType::Array);
        schema.insert("project.investigator.role-end".to_string(), FieldType::Object);
        schema.insert("project.investigator.role-end.date-parts".to_string(), FieldType::Array);
        schema.insert("project.lead-investigator".to_string(), FieldType::Array);
        schema.insert("project.lead-investigator.affiliation".to_string(), FieldType::Array);
        schema.insert("project.lead-investigator.affiliation.country".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.affiliation.name".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.affiliation.id".to_string(), FieldType::Array);
        schema.insert("project.lead-investigator.affiliation.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.affiliation.id.id".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.affiliation.id.id-type".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.family".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.given".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.ORCID".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.authenticated-orcid".to_string(), FieldType::Value);
        schema.insert("project.lead-investigator.alternate-name".to_string(), FieldType::Array);
        schema.insert("project.lead-investigator.role-start".to_string(), FieldType::Object);
        schema.insert("project.lead-investigator.role-start.date-parts".to_string(), FieldType::Array);
        schema.insert("project.lead-investigator.role-end".to_string(), FieldType::Object);
        schema.insert("project.lead-investigator.role-end.date-parts".to_string(), FieldType::Array);
        schema.insert("project.project-title".to_string(), FieldType::Array);
        schema.insert("project.project-title.title".to_string(), FieldType::Value);
        schema.insert("project.project-title.language".to_string(), FieldType::Value);
        schema.insert("project.project-description".to_string(), FieldType::Array);
        schema.insert("project.project-description.description".to_string(), FieldType::Value);
        schema.insert("project.project-description.language".to_string(), FieldType::Value);
        schema.insert("project.award-amount".to_string(), FieldType::Object);
        schema.insert("project.award-amount.amount".to_string(), FieldType::Value);
        schema.insert("project.award-amount.currency".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator".to_string(), FieldType::Array);
        schema.insert("project.co-lead-investigator.ORCID".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.affiliation".to_string(), FieldType::Array);
        schema.insert("project.co-lead-investigator.affiliation.country".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.affiliation.name".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.affiliation.id".to_string(), FieldType::Array);
        schema.insert("project.co-lead-investigator.affiliation.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.affiliation.id.id".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.affiliation.id.id-type".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.authenticated-orcid".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.family".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.given".to_string(), FieldType::Value);
        schema.insert("project.co-lead-investigator.role-end".to_string(), FieldType::Object);
        schema.insert("project.co-lead-investigator.role-end.date-parts".to_string(), FieldType::Array);
        schema.insert("project.co-lead-investigator.role-start".to_string(), FieldType::Object);
        schema.insert("project.co-lead-investigator.role-start.date-parts".to_string(), FieldType::Array);
        schema.insert("project.award-planned-end".to_string(), FieldType::Object);
        schema.insert("project.award-planned-end.date-parts".to_string(), FieldType::Array);
        schema.insert("proceedings-subject".to_string(), FieldType::Value);
        schema.insert("chair".to_string(), FieldType::Array);
        schema.insert("chair.affiliation".to_string(), FieldType::Array);
        schema.insert("chair.affiliation.name".to_string(), FieldType::Value);
        schema.insert("chair.affiliation.id".to_string(), FieldType::Array);
        schema.insert("chair.affiliation.id.asserted-by".to_string(), FieldType::Value);
        schema.insert("chair.affiliation.id.id".to_string(), FieldType::Value);
        schema.insert("chair.affiliation.id.id-type".to_string(), FieldType::Value);
        schema.insert("chair.affiliation.department".to_string(), FieldType::Array);
        schema.insert("chair.affiliation.acronym".to_string(), FieldType::Array);
        schema.insert("chair.affiliation.place".to_string(), FieldType::Array);
        schema.insert("chair.family".to_string(), FieldType::Value);
        schema.insert("chair.given".to_string(), FieldType::Value);
        schema.insert("chair.sequence".to_string(), FieldType::Value);
        schema.insert("chair.ORCID".to_string(), FieldType::Value);
        schema.insert("chair.authenticated-orcid".to_string(), FieldType::Value);
        schema.insert("chair.name".to_string(), FieldType::Value);
        schema.insert("chair.suffix".to_string(), FieldType::Value);
        schema.insert("content-updated".to_string(), FieldType::Object);
        schema.insert("content-updated.date-parts".to_string(), FieldType::Array);
        schema.insert("part-number".to_string(), FieldType::Value);
        schema.insert("type".to_string(), FieldType::Value);
        schema.insert("year".to_string(), FieldType::Value);

        schema
    };
}

#[derive(Debug, Clone)]
struct PathPattern {
    parts: Vec<PathPatternPart>,
    field_name: String,
}

impl Default for PathPattern {
    fn default() -> Self {
        Self {
            parts: Vec::new(),
            field_name: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
enum PathPatternPart {
    Field(String),
    ArrayWildcard,
}

impl PathPattern {
    fn new(field_path: &[String]) -> Self {
        let mut parts = Vec::new();
        let mut current_schema_path = String::new();

        for part in field_path {
            if !current_schema_path.is_empty() {
                current_schema_path.push_str(".");
            }
            current_schema_path.push_str(part);

            parts.push(PathPatternPart::Field(part.clone()));

            if SCHEMA_STRUCTURE.get(&current_schema_path) == Some(&FieldType::Array) {
                parts.push(PathPatternPart::ArrayWildcard);
            }
        }

        let field_name = field_path.join(".");
        PathPattern { parts, field_name }
    }

    fn apply(&self, json: &Value) -> Vec<(String, Value)> {
        let mut results = Vec::new();
        let mut to_process = vec![(json, String::new(), 0)];

        while let Some((node, current_path, parts_idx)) = to_process.pop() {
            if parts_idx >= self.parts.len() {
                 let final_value = match node {
                    Value::String(s) => Value::String(s.clone()),
                    Value::Number(n) => Value::Number(n.clone()),
                    Value::Bool(b) => Value::Bool(*b),
                    Value::Null => Value::Null,
                    Value::Object(_) | Value::Array(_) => {
                        serde_json::to_string(node)
                            .map(Value::String)
                            .unwrap_or_else(|e| {
                                warn!("Failed to serialize complex value at path '{}': {}", current_path, e);
                                Value::String("[serialization error]".to_string())
                            })
                    }
                };
                results.push((current_path, final_value));
                continue;
            }

            match &self.parts[parts_idx] {
                PathPatternPart::Field(field_name) => {
                    if let Some(value) = node.get(field_name) {
                         let new_path = if current_path.is_empty() {
                            field_name.clone()
                        } else {
                            format!("{}.{}", current_path, field_name)
                        };
                        to_process.push((value, new_path, parts_idx + 1));
                    }
                    else if field_name == "*" && node.is_object() && current_path.starts_with("relation") {
                        for (dynamic_key, value) in node.as_object().unwrap() {
                              let new_path = if current_path.is_empty() {
                                 dynamic_key.clone()
                              } else {
                                 format!("{}.{}", current_path, dynamic_key)
                              };
                             if parts_idx + 1 < self.parts.len() {
                                 to_process.push((value, new_path, parts_idx + 1));
                             }
                         }
                    }

                }
                PathPatternPart::ArrayWildcard => {
                    if let Some(array) = node.as_array() {
                        for (idx, item) in array.iter().enumerate() {
                            let array_path = format!("{}[{}]", current_path, idx);
                            to_process.push((item, array_path, parts_idx + 1));
                        }
                    } else if node.is_object() {
                         warn!("Expected array but found object at path: {}. Processing as single item.", current_path);
                         to_process.push((node, current_path.clone(), parts_idx + 1));
                    }
                }
            }
        }
        results
    }
}

fn parse_field_specifications(field_specs: &str) -> Vec<Vec<String>> {
     field_specs
        .split(',')
        .filter(|spec| !spec.trim().is_empty())
        .map(|spec| {
            spec.trim()
                .split('.')
                .map(|part| part.trim().to_string())
                .filter(|part| !part.is_empty())
                .collect::<Vec<String>>()
        })
        .filter(|parts| !parts.is_empty())
        .collect()
}

fn initialize_path_patterns(field_paths: &[Vec<String>]) -> HashMap<String, PathPattern> {
    let mut patterns = HashMap::new();

    for field_path in field_paths {
        let field_key = field_path.join(".");
        if !field_key.is_empty() {
            let pattern = PathPattern::new(field_path);
            patterns.insert(field_key, pattern);
        } else {
            warn!("Skipping invalid empty field path specification.");
        }
    }
     if patterns.keys().any(|k| k.starts_with("relation.")) {
         let relation_wildcard_parts = vec!["relation".to_string(), "*".to_string()];
         if !patterns.contains_key("relation.*") {
              info!("Adding implicit 'relation.*' pattern due to specific relation field request.");
              patterns.insert("relation.*".to_string(), PathPattern::new(&relation_wildcard_parts));
          }
         for key in patterns.keys().filter(|k| k.starts_with("relation.")).cloned().collect::<Vec<_>>() {
              let parts: Vec<String> = key.split('.').map(String::from).collect();
              if parts.len() > 2 && parts[1] != "*" {
                   let mut wildcard_parts = vec!["relation".to_string(), "*".to_string()];
                   wildcard_parts.extend_from_slice(&parts[2..]);
                   let wildcard_key = wildcard_parts.join(".");
                   if !patterns.contains_key(&wildcard_key) {
                         info!("Adding implicit wildcard pattern: {}", wildcard_key);
                         patterns.insert(wildcard_key.clone(), PathPattern::new(&wildcard_parts));
                     }
              }
          }

     }


    patterns
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

trait FileProcessor {
    fn process(&self, filepath: &Path) -> Result<FileBatchResult, (PathBuf, anyhow::Error)>;
}

struct JsonlProcessor {
    patterns: Arc<HashMap<String, PathPattern>>,
    field_paths: Vec<Vec<String>>,
    filter_member: Option<String>,
    filter_doi_prefix: Option<String>,
}

impl FileProcessor for JsonlProcessor {
    fn process(&self, filepath: &Path) -> Result<FileBatchResult, (PathBuf, anyhow::Error)> {
        let file = File::open(filepath)
             .map_err(|e| (filepath.to_path_buf(), anyhow::Error::new(e).context(format!("Failed to open file: {}", filepath.display()))))?;

        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        let mut all_field_data_for_file = Vec::new();
        let mut file_stats = FileStats::default();

        let mut current_record_fields: Vec<(String, String, String)> = Vec::new();

        let mut lines_processed = 0;
        let mut records_processed = 0;
        let mut records_missing_doi = 0;
        let mut records_missing_member = 0;
        let mut records_filtered_out = 0;
        let mut json_parsing_errors = 0;

        for (line_num, line_result) in reader.lines().enumerate() {
            lines_processed += 1;
            let line_str = match line_result {
                Ok(s) => s,
                Err(e) => {
                    warn!("Error reading line {} from {}: {}", line_num + 1, filepath.display(), e);
                    continue;
                }
            };

            if line_str.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<Value>(&line_str) {
                Ok(record) => {
                    records_processed += 1;
                    current_record_fields.clear();

                    let member_id_opt = extract_member_id(&record);
                    let doi_opt = extract_doi(&record);
                    let doi_prefix_opt = extract_doi_prefix(&record, doi_opt.as_ref());

                    if let Some(filter_m) = &self.filter_member {
                        if member_id_opt.as_ref().map_or(true, |m| &m.0 != filter_m) {
                            records_filtered_out += 1;
                            continue;
                        }
                    }
                     if let Some(filter_p) = &self.filter_doi_prefix {
                         if doi_prefix_opt.as_ref().map_or(true, |p| &p.0 != filter_p) {
                            records_filtered_out += 1;
                             continue;
                         }
                    }

                     let member_id = match member_id_opt {
                         Some(id) => id,
                         None => {
                             records_missing_member += 1;
                             continue;
                         }
                     };
                     let doi = match doi_opt {
                          Some(id) => id,
                          None => {
                              records_missing_doi += 1;
                              continue;
                          }
                     };
                     let doi_prefix = doi_prefix_opt.unwrap_or_else(|| DoiPrefix("".to_string()));

                    for field_path_vec in &self.field_paths {
                        let field_key = field_path_vec.join(".");

                         if let Some(pattern) = self.patterns.get(&field_key) {
                            let field_results = pattern.apply(&record);

                             for (subfield_path_str, value_json) in field_results {
                                let value_str = match value_json {
                                    Value::String(s) => s,
                                    Value::Number(n) => n.to_string(),
                                    Value::Bool(b) => b.to_string(),
                                    Value::Null => "".to_string(),
                                     _ => {
                                        warn!("Unexpected non-primitive value type after pattern application at path: {}", subfield_path_str);
                                        value_json.to_string()
                                    }
                                };

                                current_record_fields.push((
                                    pattern.field_name.clone(),
                                    subfield_path_str,
                                    value_str,
                                ));
                            }
                         } else {
                             warn!("Internal error: No pre-compiled pattern found for key '{}'", field_key);
                         }
                    }

                     if !current_record_fields.is_empty() {
                         file_stats.unique_dois.insert(doi.clone());
                         *file_stats.member_counts.entry(member_id.clone()).or_insert(0) += current_record_fields.len();
                         *file_stats.prefix_counts.entry(doi_prefix.clone()).or_insert(0) += current_record_fields.len();

                         for (field_name, subfield_path, value) in current_record_fields.drain(..) {
                             *file_stats.field_counts.entry(field_name.clone()).or_insert(0) += 1;
                             file_stats.total_fields_extracted += 1;

                             all_field_data_for_file.push(FieldData {
                                 doi: doi.clone(),
                                 field_name,
                                 subfield_path,
                                 value,
                                 member_id: member_id.clone(),
                                 doi_prefix: doi_prefix.clone(),
                             });
                         }
                     }

                }
                Err(e) => {
                    json_parsing_errors += 1;
                    warn!("Error parsing JSON from {}:{}: {}", filepath.display(), line_num + 1, e);
                }
            }
        }

        debug!(
            "Finished processing {}: {} lines read, {} records parsed ({} JSON errors), {} fields extracted. Skipped: {} missing DOI, {} missing Member, {} filtered out.",
            filepath.display(),
            lines_processed,
            records_processed,
            json_parsing_errors,
            all_field_data_for_file.len(),
            records_missing_doi,
            records_missing_member,
            records_filtered_out
        );

        Ok(FileBatchResult::Success(all_field_data_for_file, file_stats))
    }
}


fn extract_doi(record: &Value) -> Option<Doi> {
    record.get("DOI")
        .and_then(Value::as_str)
        .map(|s| Doi(s.to_string()))
}

fn extract_member_id(record: &Value) -> Option<MemberId> {
    record.get("member")
        .and_then(|v| {
            if v.is_string() {
                v.as_str().map(|s| MemberId(s.to_string()))
            } else if v.is_number() {
                Some(MemberId(v.to_string()))
            } else {
                None
            }
        })
}

fn extract_doi_prefix(record: &Value, doi: Option<&Doi>) -> Option<DoiPrefix> {
    record.get("prefix")
        .and_then(Value::as_str)
        .map(|s| DoiPrefix(s.to_string()))
        .or_else(|| {
             doi.and_then(|doi_val| {
                 doi_val.0.split_once('/').map(|(pfx, _)| DoiPrefix(pfx.to_string()))
             })
        })
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
                     percent = Some((vm_rss_kb? / mem_total_kb) * 100.0);
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
         let wmic_output = Command::new("wmic")
             .args(&[
                 "process",
                 "where",
                 &format!("ProcessId={}", pid),
                 "get",
                 "WorkingSetSize,",
                 "PageFileUsage",
                 "/value",
             ])
             .output()
             .ok()?;

         let output_str = String::from_utf8_lossy(&wmic_output.stdout);
         let mut rss_bytes = None;
         let mut vm_kb = None;

         for line in output_str.lines() {
             if line.starts_with("PageFileUsage=") {
                 vm_kb = line.split('=').nth(1).and_then(|s| s.trim().parse::<f64>().ok());
             } else if line.starts_with("WorkingSetSize=") {
                  rss_bytes = line.split('=').nth(1).and_then(|s| s.trim().parse::<f64>().ok());
              }
         }

         let rss_mb = rss_bytes? / (1024.0 * 1024.0);
         let vm_size_mb = vm_kb? / 1024.0;
         let mut percent = None;

          if let Ok(mem_output) = Command::new("wmic")
               .args(&["ComputerSystem", "get", "TotalPhysicalMemory", "/value"])
              .output() {
               let mem_str = String::from_utf8_lossy(&mem_output.stdout);
              if let Some(total_bytes_str) = mem_str.lines()
                  .find(|line| line.starts_with("TotalPhysicalMemory="))
                  .and_then(|line| line.split('=').nth(1)) {
                   if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() {
                       if total_bytes > 0.0 {
                           percent = Some((rss_bytes? / total_bytes) * 100.0);
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
             info!(
                 "Memory usage ({}): {:.1} MB physical (RSS), {:.1} MB virtual/commit, {} of system memory",
                 note, stats.rss_mb, stats.vm_size_mb, percent_str
             );
         } else {
             info!("Memory usage tracking not available or failed on this platform ({})", std::env::consts::OS);
         }
     }
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

trait OutputStrategy: Send {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn report_files_created(&self) -> usize;
}

struct SingleFileOutput {
    writer: Writer<File>,
    headers: Vec<String>,
    file_path: PathBuf,
}

impl SingleFileOutput {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        info!("Initializing single output file: {}", file_path.display());
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)
                 .with_context(|| format!("Failed to create directory structure for: {}", file_path.display()))?;
        }

        let headers = vec![
            "doi".to_string(),
            "field_name".to_string(),
            "subfield_path".to_string(),
            "value".to_string(),
            "member_id".to_string(),
            "doi_prefix".to_string(),
        ];

        let file = File::create(&file_path)
             .with_context(|| format!("Failed to create output file: {}", file_path.display()))?;

        let mut writer = Writer::from_writer(file);
        writer.write_record(&headers)
             .context("Failed to write header to single output file")?;
        writer.flush()
            .context("Failed to flush header to single output file")?;

        Ok(Self {
            writer,
            headers,
            file_path,
        })
    }
}

impl OutputStrategy for SingleFileOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        for field_data in batch {
            self.writer.write_record(&[
                &field_data.doi.0,
                &field_data.field_name,
                &field_data.subfield_path,
                &field_data.value,
                &field_data.member_id.0,
                &field_data.doi_prefix.0,
            ])?;
        }
        Ok(())
    }

     fn flush(&mut self) -> Result<()> {
        info!("Flushing final data to: {}", self.file_path.display());
        self.writer.flush()
            .context(format!("Failed to flush single output file: {}", self.file_path.display()))?;
        Ok(())
    }

    fn report_files_created(&self) -> usize {
        1
    }
}

struct OrganizedOutput {
    base_output_dir: PathBuf,
    current_writers: HashMap<MemberId, Writer<File>>,
    created_files: HashSet<PathBuf>,
    max_open_files: usize,
    headers: Vec<String>,
    open_file_lru: VecDeque<MemberId>,
}

impl OrganizedOutput {
    fn new<P: AsRef<Path>>(output_path: P, max_open_files: usize) -> Result<Self> {
        let path = output_path.as_ref();
        if path.exists() && !path.is_dir() {
            return Err(anyhow::anyhow!("Output path for organized output must be a directory: {}", path.display()));
        }
        fs::create_dir_all(path)
            .with_context(|| format!("Failed to create base output directory: {}", path.display()))?;
        info!("Initializing organized output in directory: {}", path.display());
        info!("Using a maximum of {} open files at once", max_open_files);

        let headers = vec![
            "doi".to_string(),
            "field_name".to_string(),
            "subfield_path".to_string(),
            "value".to_string(),
            "member_id".to_string(),
            "doi_prefix".to_string(),
        ];

        Ok(Self {
            base_output_dir: path.to_path_buf(),
            current_writers: HashMap::with_capacity(max_open_files.min(1024)),
            created_files: HashSet::new(),
            max_open_files: max_open_files.max(1),
            headers,
            open_file_lru: VecDeque::with_capacity(max_open_files),
        })
    }

    fn get_writer(&mut self, member_id: &MemberId) -> Result<&mut Writer<File>> {
         let key = member_id.clone();

         if self.current_writers.contains_key(&key) {
            if let Some(pos) = self.open_file_lru.iter().position(|x| x == &key) {
                self.open_file_lru.remove(pos);
            }
             self.open_file_lru.push_front(key.clone());
            return Ok(self.current_writers.get_mut(&key).unwrap());
        }

        while self.current_writers.len() >= self.max_open_files {
            if let Some(lru_key) = self.open_file_lru.pop_back() {
                info!("Closing LRU file for member {} to maintain max open files limit.", lru_key.0);
                 if let Some(mut writer_to_close) = self.current_writers.remove(&lru_key) {
                    if let Err(e) = writer_to_close.flush() {
                        warn!("Error flushing file for member {} before closing: {}", lru_key.0, e);
                    }
                 }
             } else {
                 error!("LRU queue empty while trying to close files. Limit: {}", self.max_open_files);
                 break;
             }
        }

        let member_file_path = self.base_output_dir.join(format!("{}.csv", key.0));
        let file_needs_header = !self.created_files.contains(&member_file_path);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&member_file_path)
            .with_context(|| format!("Failed to open/create output file for member {}: {}", key.0, member_file_path.display()))?;

        let mut csv_writer = Writer::from_writer(file);

        if file_needs_header {
             csv_writer.write_record(&self.headers)
                .with_context(|| format!("Failed to write header to: {}", member_file_path.display()))?;
            csv_writer.flush()
                .with_context(|| format!("Failed to flush header to: {}", member_file_path.display()))?;
            self.created_files.insert(member_file_path.clone());
            debug!("Created new file with header: {}", member_file_path.display());
        } else {
             debug!("Opened existing file in append mode: {}", member_file_path.display());
         }

        self.current_writers.insert(key.clone(), csv_writer);
        self.open_file_lru.push_front(key.clone());


         Ok(self.current_writers.get_mut(&key).unwrap())
    }
}


impl OutputStrategy for OrganizedOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut grouped_records: HashMap<MemberId, Vec<&FieldData>> = HashMap::new();
        for field_data in batch {
             grouped_records
                 .entry(field_data.member_id.clone())
                 .or_insert_with(Vec::new)
                 .push(field_data);
        }

        for (member_id, records) in grouped_records {
            let writer = self.get_writer(&member_id)
                .with_context(|| format!("Failed to get writer for member {}", member_id.0))?;

            for field_data in records {
                 writer.write_record(&[
                    &field_data.doi.0,
                    &field_data.field_name,
                    &field_data.subfield_path,
                    &field_data.value,
                    &field_data.member_id.0,
                    &field_data.doi_prefix.0,
                 ])?;
            }
         }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        info!("Flushing {} open CSV files...", self.current_writers.len());
        let mut flush_errors = Vec::new();
        for (member_id, writer) in self.current_writers.iter_mut() {
            if let Err(e) = writer.flush() {
                flush_errors.push(format!("Failed to flush file for member {}: {}", member_id.0, e));
            }
        }
        self.current_writers.clear();
        self.open_file_lru.clear();


        info!(
            "Total unique files created/opened during run: {}",
            self.created_files.len()
        );

        if !flush_errors.is_empty() {
            Err(anyhow::anyhow!("Errors occurred during final flush:\n - {}", flush_errors.join("\n - ")))
        } else {
            Ok(())
        }
    }

    fn report_files_created(&self) -> usize {
        self.created_files.len()
    }
}

struct CsvWriterManager {
    output_strategy: Box<dyn OutputStrategy>,
}

impl CsvWriterManager {
    fn new<P: AsRef<Path>>(output_path: P, organize: bool, max_open_files: usize) -> Result<Self> {
        let strategy: Box<dyn OutputStrategy> = if organize {
            Box::new(OrganizedOutput::new(output_path, max_open_files)?)
        } else {
            Box::new(SingleFileOutput::new(output_path)?)
        };

        Ok(Self {
            output_strategy: strategy,
        })
    }

    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        self.output_strategy.write_batch(batch)
            .context("Error writing batch via CsvWriterManager")
    }

    fn flush_all(&mut self) -> Result<()> {
        self.output_strategy.flush()
             .context("Error flushing all files via CsvWriterManager")
    }

    fn report_files_created(&self) -> usize {
        self.output_strategy.report_files_created()
    }
}

impl Drop for CsvWriterManager {
    fn drop(&mut self) {
        info!("CsvWriterManager dropping. Attempting final flush...");
        if let Err(e) = self.flush_all() {
            error!("Error flushing CSV writers during cleanup: {}", e);
        } else {
            info!("Final flush completed successfully.");
        }
    }
}

fn main() -> Result<()> {
    let start_time = Instant::now();

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

    info!("Starting Field Extractor");
    memory_usage::log_memory_usage("initial");


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

    let field_specifications = parse_field_specifications(&cli.fields);
    if field_specifications.is_empty() {
        error!("No valid field specifications provided. Check the --fields argument.");
        return Err(anyhow::anyhow!("No fields specified for extraction"));
    }
    info!("Fields to extract:");
    for spec in &field_specifications {
        info!("  - {}", spec.join("."));
    }

    info!("Pre-compiling field path patterns...");
    let path_patterns = initialize_path_patterns(&field_specifications);
    if path_patterns.is_empty() {
        error!("Failed to generate any path patterns from field specifications.");
        return Err(anyhow::anyhow!("Path pattern generation failed"));
    }
    info!("Generated {} path patterns.", path_patterns.len());
    debug!("Patterns: {:?}", path_patterns.keys());

    info!("Searching for input files in: {}", cli.input);
    let files = find_jsonl_gz_files(&cli.input)?;
    if files.is_empty() {
        warn!("No .jsonl.gz files found in the specified directory. Exiting.");
        return Ok(());
    }
    info!("Found {} files to process.", files.len());

    info!("Using target batch size for writer: {} records.", cli.batch_size);
    info!("Statistics logging interval: {} seconds.", cli.stats_interval);
    if let Some(member_filter) = &cli.member {
        info!("Filtering by member ID: {}", member_filter);
    }
    if let Some(prefix_filter) = &cli.doi_prefix {
        info!("Filtering by DOI prefix: {}", prefix_filter);
    }
    if cli.organize {
        info!("Output will be organized by member ID in directory: {}", cli.output);
        info!("Using max {} open output files.", cli.max_open_files);
    } else {
        info!("Output will be written to single file: {}", cli.output);
    }


    let progress_bar = ProgressBar::new(files.len() as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta} @ {per_sec}) {msg}")
            .expect("Failed to create progress bar template")
            .progress_chars("=> "),
    );
    progress_bar.set_message("Starting processing...");

    let stats = Arc::new(IncrementalStats::new());

    let channel_capacity = (num_threads * 2).max(4);
    let (batch_sender, batch_receiver): (Sender<Vec<FieldData>>, Receiver<Vec<FieldData>>) = bounded(channel_capacity);
    info!("Using writer channel with capacity: {}", channel_capacity);

    let output_path_clone = cli.output.clone();
    let organize_clone = cli.organize;
    let max_open_files_clone = cli.max_open_files;
    let writer_thread = thread::spawn(move || -> Result<usize> {
        info!("Writer thread started.");
        let mut csv_writer_manager = CsvWriterManager::new(
            &output_path_clone,
            organize_clone,
            max_open_files_clone
        )?;

        let mut batches_written = 0;
        let mut records_written = 0;

        for batch in batch_receiver {
            if !batch.is_empty() {
                 let count = batch.len();
                 if let Err(e) = csv_writer_manager.write_batch(&batch) {
                     error!("Writer thread error writing batch: {}", e);
                 } else {
                      batches_written += 1;
                      records_written += count;
                      debug!("Writer thread wrote batch {} ({} records)", batches_written, count);
                  }
            }
        }

        info!("Writer thread finished receiving. Wrote {} records in {} batches.", records_written, batches_written);
         Ok(csv_writer_manager.report_files_created())
    });


    let stats_thread_running = Arc::new(std::sync::Mutex::new(true));
    let stats_interval_duration = Duration::from_secs(cli.stats_interval);
    let stats_clone_for_thread = Arc::clone(&stats);
    let stats_thread_running_clone = Arc::clone(&stats_thread_running);
    let stats_thread = thread::spawn(move || {
        info!("Stats logging thread started.");
        let mut last_log_time = Instant::now();
        loop {
             if let Ok(running) = stats_thread_running_clone.lock() {
                 if !*running {
                     info!("Stats thread received stop signal.");
                     break;
                 }
             } else {
                 error!("Stats thread failed to lock running flag. Exiting.");
                 break;
             }

             thread::sleep(Duration::from_millis(500));

            if last_log_time.elapsed() >= stats_interval_duration {
                memory_usage::log_memory_usage("periodic check");
                 stats_clone_for_thread.log_current_stats();
                last_log_time = Instant::now();
            }
        }
        info!("Stats logging thread finished.");
    });

    info!("Starting parallel file processing...");
    let patterns_arc = Arc::new(path_patterns);

    let processor = Arc::new(JsonlProcessor {
        patterns: patterns_arc,
        field_paths: field_specifications.clone(),
        filter_member: cli.member,
        filter_doi_prefix: cli.doi_prefix,
    });

    let processing_results: Vec<Result<FileStats, (PathBuf, anyhow::Error)>> = files
        .par_iter()
        .map(|filepath| {
            let processor_ref = Arc::clone(&processor);
            let sender_clone = batch_sender.clone();
            let pb_clone = progress_bar.clone();
            let target_batch_size = cli.batch_size;

            let process_start_time = Instant::now();

            let file_result = processor_ref.process(filepath);
            let duration = process_start_time.elapsed();

            let file_name_msg = filepath.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| filepath.display().to_string());

             pb_clone.inc(1);

            match file_result {
                Ok(FileBatchResult::Success(mut data_batch, file_stats)) => {
                    let num_extracted = data_batch.len();
                     pb_clone.set_message(format!("OK: {} ({} fields, {})", file_name_msg, num_extracted, format_elapsed(duration)));

                    if !data_batch.is_empty() {
                         for chunk in data_batch.chunks(target_batch_size) {
                             if let Err(e) = sender_clone.send(chunk.to_vec()) {
                                 error!("Failed to send batch from {} to writer thread: {}. Writer likely panicked.", filepath.display(), e);
                                return Err((filepath.to_path_buf(), anyhow::anyhow!("Writer channel closed unexpectedly")));
                            }
                         }
                     }
                    Ok(file_stats)
                }
                Err((path, e)) => {
                    error!("Initial error processing file {}: {}", path.display(), e);
                     pb_clone.set_message(format!("ERR: {} (Initial Error)", file_name_msg));
                    Err((path, e))
                }
                Ok(FileBatchResult::Error(path, e)) => {
                    error!("Processing error within file {}: {}", path.display(), e);
                    pb_clone.set_message(format!("ERR: {} (Processing Error)", file_name_msg));
                    Err((path,e))
                }

            }
        })
        .collect();

    info!("File processing complete. Aggregating final stats...");
    progress_bar.set_message("Aggregating stats...");

    drop(batch_sender);

    let mut files_with_errors = Vec::new();
    for result in processing_results {
        match result {
            Ok(file_stats) => {
                stats.aggregate_file_stats(file_stats);
            }
            Err((path, _e)) => {
                stats.increment_error_files();
                files_with_errors.push(path);
            }
        }
    }

    progress_bar.finish_with_message(format!(
        "Processing finished. {} files OK, {} errors.",
        stats.processed_files_ok.load(Ordering::Relaxed),
        stats.processed_files_error.load(Ordering::Relaxed)
    ));

    info!("Waiting for writer thread to finish writing remaining batches...");
    let files_created_result = writer_thread.join();

    let files_created = match files_created_result {
         Ok(Ok(count)) => {
            info!("Writer thread finished successfully.");
            Some(count)
         },
         Ok(Err(e)) => {
              error!("Writer thread returned an error: {}", e);
              None
          }
         Err(e) => {
             error!("Writer thread panicked: {:?}", e);
             None
         }
    };

    info!("Signaling stats thread to stop...");
    {
        if let Ok(mut running_guard) = stats_thread_running.lock() {
            *running_guard = false;
        } else {
            error!("Failed to lock stats thread running flag to signal stop.");
        }
    }

    info!("Waiting for stats thread to finish...");
    if let Err(e) = stats_thread.join() {
        error!("Error joining stats thread: {:?}", e);
    } else {
        info!("Stats thread joined successfully.");
    }

    info!("-------------------- FINAL SUMMARY --------------------");
    let total_runtime = start_time.elapsed();
    info!("Total execution time: {}", format_elapsed(total_runtime));
    info!("Input files found: {}", files.len());

    let final_stats = stats.get_final_stats();

    info!("Files processed successfully: {}", final_stats.processed_files_ok);
    if !files_with_errors.is_empty() {
        warn!("Files with processing errors: {}", files_with_errors.len());
        for err_file in files_with_errors.iter().take(10) {
            warn!("  - {}", err_file.display());
        }
        if files_with_errors.len() > 10 {
            warn!("  ... (and {} more)", files_with_errors.len() - 10);
        }
    }
    info!("Total field records extracted: {}", final_stats.total_field_records);
    info!("Unique DOIs encountered: {}", final_stats.unique_dois);
    info!("Unique Members encountered: {}", final_stats.unique_members.len());
    info!("Unique DOI Prefixes encountered: {}", final_stats.unique_prefixes.len());

    info!("Final Field breakdown:");
    let mut final_sorted_fields: Vec<_> = final_stats.unique_fields.iter().collect();
    final_sorted_fields.sort_by_key(|&(_, count)| std::cmp::Reverse(*count));
    for (field, count) in final_sorted_fields.iter().take(20) {
        info!("  - {}: {} records", field, count);
    }
    if final_sorted_fields.len() > 20 {
        info!("  ... ({} more fields)", final_sorted_fields.len() - 20);
    }

    if !final_stats.unique_members.is_empty() && final_stats.unique_members.len() < 50 {
        info!("Final Member statistics:");
        let mut sorted_members: Vec<_> = final_stats.unique_members.iter().collect();
        sorted_members.sort_by_key(|&(_, count)| std::cmp::Reverse(*count));
        for (member, count) in sorted_members {
            info!("  - Member {}: {} records", member.0, count);
        }
    } else if final_stats.unique_members.len() >= 50 {
        info!("(Skipping detailed stats for {} members)", final_stats.unique_members.len());
    }

    if let Some(count) = files_created {
         if cli.organize {
            info!("Total unique output files created/opened: {}", count);
         } else {
             info!("Output written to: {}", cli.output);
         }
    } else {
         error!("Could not determine number of files created by writer thread.");
     }

    memory_usage::log_memory_usage("final");
    info!("Extraction process finished.");
    info!("-------------------------------------------------------");

    Ok(())
}
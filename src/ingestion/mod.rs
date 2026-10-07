//! Structured preparation and auditable foreground ingestion orchestration.
mod service;
use crate::chunking::{ChunkPolicy, DocumentIndex, index_document};
use crate::{
    crawler::CrawlReport,
    extraction::{ExtractionOutcome, extract},
};
use serde::{Deserialize, Serialize};
pub use service::{IngestionProgress, IngestionStage, IngestionStatus, ingest, ingest_lexical};
use std::time::SystemTime;

/// The parallel indexes are private so a caller cannot mismatch documents and
/// their derived data. Only successful extraction outcomes acquire an index.
#[derive(Debug)]
pub struct PreparedCrawl {
    pub(crate) batch: ExtractionBatch,
    pub(crate) indexes: Vec<Option<DocumentIndex>>,
}

pub fn prepare_crawl(batch: ExtractionBatch, policy: ChunkPolicy) -> anyhow::Result<PreparedCrawl> {
    let indexes = batch
        .outcomes
        .iter()
        .map(|outcome| match outcome {
            ExtractionOutcome::Extracted(document) => index_document(document, policy).map(Some),
            ExtractionOutcome::Rejected { .. } => Ok(None),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(PreparedCrawl { batch, indexes })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractionBatch {
    pub source_id: String,
    pub crawl_id: String,
    pub started_at: SystemTime,
    pub finished_at: SystemTime,
    /// Includes explicit rejection outcomes for failed fetches and extraction failures.
    pub outcomes: Vec<ExtractionOutcome>,
    pub blocked: Vec<crate::crawler::BlockedUrl>,
    pub dropped_pages: u64,
    pub audit_overflow: bool,
}

impl ExtractionBatch {
    pub fn delivery_complete(&self) -> bool {
        self.dropped_pages == 0 && !self.audit_overflow
    }
}

/// Moves raw pages into extraction without dropping failed or incomplete-run metadata.
pub fn extract_crawl(report: CrawlReport) -> ExtractionBatch {
    ExtractionBatch {
        source_id: report.source_id,
        crawl_id: report.crawl_id,
        started_at: report.started_at,
        finished_at: report.finished_at,
        outcomes: report.pages.into_iter().map(extract).collect(),
        blocked: report.blocked,
        dropped_pages: report.dropped_pages,
        audit_overflow: report.audit_overflow,
    }
}

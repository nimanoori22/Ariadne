//! Structured preparation and auditable foreground ingestion orchestration.
mod incremental;
mod service;
mod site;
use crate::chunking::{ChunkPolicy, DocumentIndex, index_document};
use crate::{
    crawler::CrawlReport,
    extraction::{ExtractionOutcome, extract},
};
pub(crate) use incremental::{RecrawlPlan, prepare_incremental};
use serde::{Deserialize, Serialize};
pub use service::{
    IngestionOperation, IngestionProgress, IngestionStage, IngestionStatus, ingest, ingest_lexical,
    recrawl, recrawl_lexical, reprocess_lexical,
};
pub use site::crawl_site;
use std::time::SystemTime;

/// The parallel indexes are private so a caller cannot mismatch documents and
/// their derived data. Only successful extraction outcomes acquire an index.
#[derive(Debug)]
pub struct PreparedCrawl {
    pub(crate) batch: ExtractionBatch,
    pub(crate) indexes: Vec<Option<PreparedDocument>>,
    pub(crate) recrawl: Option<RecrawlPlan>,
}

#[derive(Debug)]
pub(crate) struct PreparedDocument {
    pub index: DocumentIndex,
    pub graph: crate::graph::DocumentGraph,
}

fn prepare_document(
    document: &crate::extraction::ExtractedDocument,
    policy: ChunkPolicy,
) -> anyhow::Result<PreparedDocument> {
    let index = index_document(document, policy)?;
    let graph = crate::graph::derive(document, &index.chunks)?;
    Ok(PreparedDocument { index, graph })
}

pub fn prepare_crawl(batch: ExtractionBatch, policy: ChunkPolicy) -> anyhow::Result<PreparedCrawl> {
    let indexes = batch
        .outcomes
        .iter()
        .map(|outcome| match outcome {
            ExtractionOutcome::Extracted(document) => prepare_document(document, policy).map(Some),
            ExtractionOutcome::Rejected { .. } => Ok(None),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(PreparedCrawl {
        batch,
        indexes,
        recrawl: None,
    })
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
    #[serde(default)]
    pub discovery: crate::crawler::DiscoveryReport,
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
        discovery: report.discovery,
    }
}

//! Foreground orchestration. A completed crawl remains usable if embeddings fail.
use std::{future::Future, time::SystemTime};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use super::{extract_crawl, prepare_crawl, prepare_incremental};
use crate::{
    chunking::ChunkPolicy,
    crawler::{CrawlReport, CrawlRequest, crawl, crawl_with_cache},
    embeddings::{
        EmbeddingCoverage, EmbeddingProvider, EmbeddingReport, EmbeddingSpace, index_source,
    },
    storage::{CrawlRun, KnowledgeStore, Source},
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionStatus {
    Running,
    Completed,
    Partial,
    Failed,
    Interrupted,
    Cancelled,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionStage {
    Crawl,
    Prepare,
    Persist,
    Embeddings,
    Complete,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionOperation {
    #[default]
    Ingest,
    Recrawl,
    Reprocess,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestionProgress {
    #[serde(default)]
    pub operation: IngestionOperation,
    pub status: IngestionStatus,
    pub stage: IngestionStage,
    pub started_at: SystemTime,
    pub updated_at: SystemTime,
    pub finished_at: Option<SystemTime>,
    pub chunk_policy: ChunkPolicy,
    pub embeddings_requested: bool,
    pub embedding_space: Option<EmbeddingSpace>,
    pub embedding_report: Option<EmbeddingReport>,
    pub embedding_coverage: Option<EmbeddingCoverage>,
    pub error: Option<String>,
}

/// Registration is idempotent; a crawl ID must be new. Provider initialization is
/// deferred until structured text has been committed. Its identity is audited.
pub async fn ingest<P, F, Fut>(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
    provider: F,
) -> Result<CrawlRun>
where
    P: EmbeddingProvider,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<P>>,
{
    with_embeddings(
        store,
        source,
        request,
        policy,
        provider,
        IngestionOperation::Ingest,
    )
    .await
}

pub async fn recrawl<P, F, Fut>(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
    provider: F,
) -> Result<CrawlRun>
where
    P: EmbeddingProvider,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<P>>,
{
    with_embeddings(
        store,
        source,
        request,
        policy,
        provider,
        IngestionOperation::Recrawl,
    )
    .await
}

async fn with_embeddings<P, F, Fut>(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
    provider: F,
    operation: IngestionOperation,
) -> Result<CrawlRun>
where
    P: EmbeddingProvider,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<P>>,
{
    let mut progress = ingest_text(store, source, request.clone(), policy, true, operation).await?;
    progress.stage = IngestionStage::Embeddings;
    checkpoint(store, &request, &mut progress).await?;
    let result = async {
        let provider = provider().await.context("connect embedding provider")?;
        progress.embedding_space = Some(provider.space().clone());
        checkpoint(store, &request, &mut progress).await?;
        progress.embedding_report = Some(index_source(store, &provider, &request.source_id).await?);
        progress.embedding_coverage = Some(
            store
                .embedding_coverage(provider.space(), &request.source_id)
                .await?,
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = result {
        // The crawl is already committed. Preserve lexical availability and audit
        // this partial success rather than rewriting the crawl as failed.
        progress.status = IngestionStatus::Partial;
        progress.error = Some(format!("{error:#}").chars().take(1000).collect());
        progress.finished_at = Some(SystemTime::now());
        checkpoint(store, &request, &mut progress).await?;
        return Err(error);
    }
    let coverage = progress
        .embedding_coverage
        .as_ref()
        .expect("coverage assigned");
    let partial = coverage.failed + coverage.pending + coverage.missing > 0;
    finish(store, &request, progress, partial).await
}

pub async fn ingest_lexical(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
) -> Result<CrawlRun> {
    let progress = ingest_text(
        store,
        source,
        request.clone(),
        policy,
        false,
        IngestionOperation::Ingest,
    )
    .await?;
    finish(store, &request, progress, false).await
}

pub async fn recrawl_lexical(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
) -> Result<CrawlRun> {
    let progress = ingest_text(
        store,
        source,
        request.clone(),
        policy,
        false,
        IngestionOperation::Recrawl,
    )
    .await?;
    finish(store, &request, progress, false).await
}

/// Re-extract retained content without HTTP requests. Removed documents stay
/// unavailable until a successful network fetch restores them.
pub async fn reprocess_lexical(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
) -> Result<CrawlRun> {
    let progress = ingest_text(
        store,
        source,
        request.clone(),
        policy,
        false,
        IngestionOperation::Reprocess,
    )
    .await?;
    finish(store, &request, progress, false).await
}

async fn ingest_text(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
    embeddings_requested: bool,
    operation: IngestionOperation,
) -> Result<IngestionProgress> {
    request.validate()?;
    ensure!(
        request.source_id == source.id && request.seed == source.root_url,
        "crawl request must match the registered source"
    );
    ensure!(
        (1..=1_000_000).contains(&policy.target_chars),
        "chunk budget must be between 1 and 1000000"
    );
    store.register_source(source).await?;
    store.begin_crawl(&request).await?;
    let now = SystemTime::now();
    let mut progress = IngestionProgress {
        operation,
        status: IngestionStatus::Running,
        stage: IngestionStage::Crawl,
        started_at: now,
        updated_at: now,
        finished_at: None,
        chunk_policy: policy,
        embeddings_requested,
        embedding_space: None,
        embedding_report: None,
        embedding_coverage: None,
        error: None,
    };
    let result = async {
        checkpoint(store, &request, &mut progress).await?;
        // Spider's crawl future is large; keep it on the heap so adding the
        // incremental branch does not exhaust ordinary Tokio worker stacks.
        let report = match operation {
            IngestionOperation::Ingest => Box::pin(crawl(request.clone())).await?,
            IngestionOperation::Recrawl => {
                Box::pin(crawl_with_cache(
                    request.clone(),
                    store.page_cache(&request.source_id).await?,
                ))
                .await?
            }
            IngestionOperation::Reprocess => {
                let started_at = SystemTime::now();
                let cache = store.page_cache(&request.source_id).await?;
                let mut pages = Vec::new();
                for raw in cache.urls() {
                    if pages.len() >= request.max_pages as usize {
                        break;
                    }
                    let url = url::Url::parse(raw)?;
                    if request.scope.contains(&url)
                        && let Some(mut page) = cache.get(raw).await?
                    {
                        page.crawl_id = request.crawl_id.clone();
                        pages.push(page);
                    }
                }
                CrawlReport {
                    discovered_urls: vec![],
                    source_id: request.source_id.clone(),
                    crawl_id: request.crawl_id.clone(),
                    started_at,
                    finished_at: SystemTime::now(),
                    pages,
                    blocked: vec![],
                    dropped_pages: 0,
                    audit_overflow: false,
                    discovery: Default::default(),
                }
            }
        };
        progress.stage = IngestionStage::Prepare;
        checkpoint(store, &request, &mut progress).await?;
        let prepared = if operation == IngestionOperation::Ingest {
            tokio::task::spawn_blocking(move || prepare_crawl(extract_crawl(report), policy))
                .await
                .context("join document preparation")??
        } else {
            prepare_incremental(
                store,
                report,
                policy,
                operation == IngestionOperation::Reprocess,
            )
            .await?
        };
        progress.stage = IngestionStage::Persist;
        checkpoint(store, &request, &mut progress).await?;
        store.finish_prepared_crawl(prepared).await
    }
    .await;
    if let Err(error) = result {
        progress.status = IngestionStatus::Failed;
        progress.error = Some(format!("{error:#}").chars().take(1000).collect());
        progress.finished_at = Some(SystemTime::now());
        store
            .fail_crawl(
                &request.source_id,
                &request.crawl_id,
                progress.error.as_deref().unwrap(),
            )
            .await?;
        checkpoint(store, &request, &mut progress).await?;
        return Err(error);
    }
    Ok(progress)
}

async fn checkpoint(
    store: &KnowledgeStore,
    request: &CrawlRequest,
    progress: &mut IngestionProgress,
) -> Result<()> {
    progress.updated_at = SystemTime::now();
    tracing::info!(source_id = %request.source_id, crawl_id = %request.crawl_id,
        stage = ?progress.stage, status = ?progress.status, "ingestion progress");
    store
        .set_ingestion_progress(&request.source_id, &request.crawl_id, progress)
        .await
}

async fn finish(
    store: &KnowledgeStore,
    request: &CrawlRequest,
    mut progress: IngestionProgress,
    partial: bool,
) -> Result<CrawlRun> {
    let run = store
        .get_crawl(&request.source_id, &request.crawl_id)
        .await?
        .context("crawl missing")?;
    let summary = run
        .summary
        .as_ref()
        .context("completed crawl missing summary")?;
    let incomplete = summary["rejected_count"].as_u64().unwrap_or(0) > 0
        || summary["incremental"]["unvisited_known"]
            .as_u64()
            .unwrap_or(0)
            > 0
        || summary["document_count"].as_u64().unwrap_or(0) == 0
        || summary["delivery_complete"] != true;
    progress.status = if partial || incomplete {
        IngestionStatus::Partial
    } else {
        IngestionStatus::Completed
    };
    progress.stage = IngestionStage::Complete;
    progress.finished_at = Some(SystemTime::now());
    checkpoint(store, request, &mut progress).await?;
    store
        .get_crawl(&request.source_id, &request.crawl_id)
        .await?
        .context("crawl missing")
}

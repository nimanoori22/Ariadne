//! Foreground orchestration. A completed crawl remains usable if embeddings fail.
use std::{future::Future, time::SystemTime};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use super::{extract_crawl, prepare_crawl};
use crate::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, crawl},
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

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestionProgress {
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
    let mut progress = ingest_text(store, source, request.clone(), policy, true).await?;
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
    let progress = ingest_text(store, source, request.clone(), policy, false).await?;
    finish(store, &request, progress, false).await
}

async fn ingest_text(
    store: &KnowledgeStore,
    source: Source,
    request: CrawlRequest,
    policy: ChunkPolicy,
    embeddings_requested: bool,
) -> Result<IngestionProgress> {
    request.validate()?;
    ensure!(
        request.source_id == source.id && request.seed == source.root_url,
        "crawl request must match the registered source"
    );
    ensure!(policy.target_chars > 0, "chunk budget must be positive");
    store.register_source(source).await?;
    store.begin_crawl(&request).await?;
    let now = SystemTime::now();
    let mut progress = IngestionProgress {
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
        let report = crawl(request.clone()).await?;
        progress.stage = IngestionStage::Prepare;
        checkpoint(store, &request, &mut progress).await?;
        let prepared =
            tokio::task::spawn_blocking(move || prepare_crawl(extract_crawl(report), policy))
                .await
                .context("join document preparation")??;
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

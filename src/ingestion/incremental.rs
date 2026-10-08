use super::{ExtractionBatch, PreparedCrawl, prepare_document};
use crate::{
    chunking::{
        CHUNKING_VERSION, ChunkPolicy, IndexMetadata, NORMALIZATION_VERSION, content_hashes,
    },
    crawler::{CrawlReport, PageOutcome, PageState},
    extraction::{EXTRACTION_VERSION, ExtractedDocument, ExtractionOutcome, extract},
    storage::KnowledgeStore,
};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use url::Url;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReuseKind {
    NotModified,
    RawUnchanged,
    NormalizedUnchanged,
}

#[derive(Debug)]
pub(crate) struct ReusedDocument {
    pub page: PageOutcome,
    pub url: Url,
    pub indexing: IndexMetadata,
    pub kind: ReuseKind,
    pub refreshed: Option<Box<ExtractedDocument>>,
}

#[derive(Debug, Default)]
pub(crate) struct RecrawlPlan {
    pub reused: Vec<ReusedDocument>,
    pub removed: Vec<PageOutcome>,
    pub audit_overrides: Vec<PageOutcome>,
    pub added: usize,
    pub changed: usize,
    pub reprocessed: usize,
    pub offline: bool,
    pub validations: HashMap<String, Value>,
    pub known_documents: usize,
    pub unvisited_known: usize,
}

fn compatible(index: &IndexMetadata, policy: ChunkPolicy) -> bool {
    index.extraction_version == EXTRACTION_VERSION
        && index.chunking_version == CHUNKING_VERSION
        && index.hashes.normalization_version == NORMALIZATION_VERSION
        && index.policy == policy
}

fn usable_headers(page: &PageOutcome) -> bool {
    page.headers
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case("content-type"))
        .all(|(_, value)| {
            let value = String::from_utf8_lossy(value).to_ascii_lowercase();
            let mut parts = value.split(';');
            matches!(
                parts.next().unwrap_or("").trim(),
                "text/html" | "application/xhtml+xml"
            ) && parts.all(|p| {
                p.trim().strip_prefix("charset=").is_none_or(|v| {
                    matches!(
                        v.trim_matches(['\'', '"']).trim(),
                        "utf-8" | "utf8" | "us-ascii"
                    )
                })
            })
        })
}

pub(crate) async fn prepare_incremental(
    store: &KnowledgeStore,
    report: CrawlReport,
    policy: ChunkPolicy,
    force: bool,
) -> Result<PreparedCrawl> {
    let mut plan = RecrawlPlan {
        offline: force,
        known_documents: store.document_count(&report.source_id, !force).await?,
        ..Default::default()
    };
    let mut visited = HashSet::new();
    let mut outcomes = Vec::new();
    let mut indexes = Vec::new();
    for mut page in report.pages {
        let mut url = Url::parse(&page.final_url).context("invalid fetched URL")?;
        url.set_fragment(None);
        let snapshot = store.recrawl_snapshot(&report.source_id, &url).await?;
        if snapshot.is_some() {
            visited.insert(url.to_string());
        }
        let available = snapshot.as_ref().is_none_or(|s| {
            s.revalidation
                .as_ref()
                .is_none_or(|v| v["availability"] != "removed")
        });
        if !force
            && matches!(page.status, 404 | 410)
            && page.state == PageState::HttpFailure
            && page.requested_url == url.as_str()
            && snapshot.is_some()
        {
            plan.removed.push(page);
            continue;
        }
        let old = snapshot.as_ref().and_then(|s| s.indexing.as_ref());
        if force {
            if let Some(saved) = snapshot.as_ref().and_then(|s| s.revalidation.as_ref()) {
                plan.validations.insert(url.to_string(), saved.clone());
            } else {
                plan.validations.insert(
                    url.to_string(),
                    json!({"availability":"active",
                    "last_checked_at":page.fetched_at,"last_checked_crawl_id":null,
                    "status":page.status,"headers":page.headers}),
                );
            }
        }
        let graph_compatible = snapshot
            .as_ref()
            .is_some_and(|s| s.graph_version.as_deref() == Some(crate::graph::GRAPH_VERSION));
        let can_reuse =
            !force && available && old.is_some_and(|i| compatible(i, policy)) && graph_compatible;
        if page.state == PageState::NotModified {
            if can_reuse {
                plan.reused.push(ReusedDocument {
                    page,
                    url,
                    indexing: old.unwrap().clone(),
                    kind: ReuseKind::NotModified,
                    refreshed: None,
                });
                continue;
            }
            // A 304 validates bytes, not the extraction/chunking algorithm.
            let saved = store
                .get_document(&report.source_id, &url)
                .await?
                .context("304 without retained document")?;
            let actual = page;
            page = saved.page;
            page.crawl_id = report.crawl_id.clone();
            if let Some(headers) = snapshot
                .as_ref()
                .and_then(|s| s.revalidation.as_ref())
                .and_then(|v| v["headers"].as_array())
            {
                page.headers = serde_json::from_value(Value::Array(headers.clone()))?;
            }
            for (name, value) in &actual.headers {
                if [
                    "etag",
                    "last-modified",
                    "cache-control",
                    "vary",
                    "content-location",
                ]
                .contains(&name.as_str())
                {
                    page.headers
                        .retain(|(old, _)| !old.eq_ignore_ascii_case(name));
                    page.headers.push((name.clone(), value.clone()));
                }
            }
            plan.validations.insert(
                url.to_string(),
                json!({"availability":"active",
                "last_checked_at":actual.fetched_at,"last_checked_crawl_id":actual.crawl_id,
                "status":304,"headers":page.headers}),
            );
            plan.audit_overrides.push(actual);
        } else if can_reuse
            && page.state == PageState::Fetched
            && page.status == 200
            && usable_headers(&page)
            && old.is_some_and(|i| {
                i.hashes.raw_sha256 == format!("{:x}", Sha256::digest(&page.raw_body))
            })
        {
            plan.reused.push(ReusedDocument {
                page,
                url,
                indexing: old.unwrap().clone(),
                kind: ReuseKind::RawUnchanged,
                refreshed: None,
            });
            continue;
        }
        // Sequential bounded blocking jobs keep HTML parsing off async workers.
        let outcome = tokio::task::spawn_blocking(move || extract(page)).await?;
        let outcome = match outcome {
            ExtractionOutcome::Extracted(document) => {
                let hashes = content_hashes(&document)?;
                if can_reuse
                    && old.is_some_and(|i| i.hashes.normalized_sha256 == hashes.normalized_sha256)
                {
                    let mut indexing = old.unwrap().clone();
                    indexing.hashes = hashes;
                    // Preserve current raw bytes even when only boilerplate changed.
                    let audit_page = document.page.clone();
                    plan.reused.push(ReusedDocument {
                        page: audit_page,
                        url,
                        indexing,
                        kind: ReuseKind::NormalizedUnchanged,
                        refreshed: Some(document),
                    });
                    continue;
                }
                if snapshot.is_none() {
                    plan.added += 1;
                } else if force || !graph_compatible || old.is_none_or(|i| !compatible(i, policy)) {
                    plan.reprocessed += 1;
                } else {
                    plan.changed += 1;
                }
                let (document, index) = tokio::task::spawn_blocking(move || {
                    prepare_document(&document, policy).map(|index| (document, index))
                })
                .await??;
                indexes.push(Some(index));
                ExtractionOutcome::Extracted(document)
            }
            rejected => {
                indexes.push(None);
                rejected
            }
        };
        outcomes.push(outcome);
    }
    plan.unvisited_known = plan.known_documents.saturating_sub(visited.len());
    Ok(PreparedCrawl {
        batch: ExtractionBatch {
            source_id: report.source_id,
            crawl_id: report.crawl_id,
            started_at: report.started_at,
            finished_at: report.finished_at,
            outcomes,
            blocked: report.blocked,
            dropped_pages: report.dropped_pages,
            audit_overflow: report.audit_overflow,
            discovery: report.discovery,
        },
        indexes,
        recrawl: Some(plan),
    })
}

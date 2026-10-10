//! Full-site HTML crawl. Spider fetches and discovers links; storage owns a
//! durable frontier, so total coverage has no page cap and bodies stay bounded.
use super::{ExtractionBatch, extract_crawl, prepare_crawl};
use crate::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, PageOutcome, PageState, crawl},
    storage::{CrawlRun, KnowledgeStore, Source},
};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use std::time::SystemTime;
use url::Url;

fn html_candidate(url: &Url) -> bool {
    let name = url.path().rsplit('/').next().unwrap_or_default();
    match name.rsplit_once('.') {
        None => true,
        Some((_, extension)) => matches!(
            extension.to_ascii_lowercase().as_str(),
            "htm" | "html" | "xhtml" | "shtml" | "php" | "asp" | "aspx" | "jsp"
        ),
    }
}

pub async fn crawl_site(
    store: &KnowledgeStore,
    source: Source,
    mut request: CrawlRequest,
    policy: ChunkPolicy,
    resume: bool,
) -> Result<CrawlRun> {
    ensure!(
        request.seed == source.root_url && request.source_id == source.id,
        "site request must match registered source"
    );
    ensure!(
        !request.browser_fallback,
        "full-site mode currently supports static HTML only"
    );
    request.max_pages = 20;
    request.subscription_capacity = 64;
    request.validate()?;
    store.register_source(source).await?;
    if resume {
        store
            .resume_site(&request.source_id, &request.crawl_id)
            .await?;
    } else {
        store.begin_crawl(&request).await?;
        store
            .seed_site(&request.source_id, &request.crawl_id, &request.seed)
            .await?;
    }
    let result = async {
        loop {
            let selected = store
                .pending_site_urls(&request.source_id, &request.crawl_id, 20)
                .await?;
            let finish = selected.is_empty();
            let (selected, skipped): (Vec<_>, Vec<_>) =
                selected.into_iter().partition(html_candidate);
            // Older checkpoints may already contain links to downloadable files.
            let mut visited: Vec<_> = skipped
                .into_iter()
                .map(|url| json!({"url":url,"status":"skipped","reason":"non-HTML URL"}))
                .collect();
            let (batch, links) = if selected.is_empty() {
                let now = SystemTime::now();
                (
                    ExtractionBatch {
                        source_id: request.source_id.clone(),
                        crawl_id: request.crawl_id.clone(),
                        started_at: now,
                        finished_at: now,
                        outcomes: vec![],
                        blocked: vec![],
                        dropped_pages: 0,
                        audit_overflow: false,
                        discovery: Default::default(),
                    },
                    vec![],
                )
            } else {
                let mut fetch = request.clone();
                fetch.seed = selected[0].clone();
                fetch.selected_urls = selected.clone();
                fetch.discovery = request.discovery && selected.iter().any(|u| u == &request.seed);
                let mut report = Box::pin(crawl(fetch)).await?;
                ensure!(
                    report.delivery_complete(),
                    "site batch lost delivery; pending URLs retained for resume"
                );
                for url in selected {
                    let page = report
                        .pages
                        .iter()
                        .find(|p| p.requested_url == url.as_str());
                    let robots = report.blocked.iter().any(|b| {
                        b.requested_url == url.as_str()
                            && b.reason == crate::crawler::BlockReason::Robots
                    });
                    let status = match page {
                        Some(p) if p.state == PageState::Fetched => "done",
                        Some(_) => "failed",
                        None if robots => "blocked",
                        None => "failed",
                    };
                    let reason = match status {
                        "done" => None,
                        _ if robots => Some("robots.txt"),
                        _ => Some("fetch failed or no page delivered"),
                    };
                    visited.push(json!({"url": url, "status": status, "reason": reason}));
                    if page.is_none() {
                        report.pages.push(PageOutcome {
                            source_id: request.source_id.clone(),
                            crawl_id: request.crawl_id.clone(),
                            requested_url: url.to_string(),
                            final_url: url.to_string(),
                            fetched_at: SystemTime::now(),
                            status: 0,
                            headers: vec![],
                            raw_body: vec![],
                            content_truncated: false,
                            rendering: None,
                            state: if robots {
                                PageState::Blocked
                            } else {
                                PageState::TransportFailure {
                                    message: "no page delivered".into(),
                                }
                            },
                        });
                    }
                }
                let links = std::mem::take(&mut report.discovered_urls)
                    .into_iter()
                    .filter(|raw| Url::parse(raw).is_ok_and(|url| html_candidate(&url)))
                    .collect::<Vec<_>>();
                (extract_crawl(report), links)
            };
            let prepared = tokio::task::spawn_blocking(move || prepare_crawl(batch, policy))
                .await
                .context("prepare site batch")??;
            store
                .commit_prepared_crawl(
                    prepared,
                    Some(json!({"visited": visited, "links": links, "finish": finish})),
                )
                .await?;
            let coverage = store
                .site_coverage(&request.source_id, &request.crawl_id)
                .await?;
            tracing::info!(source_id=%request.source_id, crawl_id=%request.crawl_id,
                coverage=%coverage, "site crawl checkpoint");
            if finish {
                break;
            }
        }
        store
            .get_crawl(&request.source_id, &request.crawl_id)
            .await?
            .context("site crawl missing")
    }
    .await;
    if let Err(error) = &result {
        let _ = store
            .fail_crawl(&request.source_id, &request.crawl_id, &format!("{error:#}"))
            .await;
    }
    result
}

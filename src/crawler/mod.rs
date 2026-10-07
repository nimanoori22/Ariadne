//! Bounded static crawling through Spider. Engine-specific types stay here.
use std::{
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use spider::{page::Page, website::Website};
use tokio::sync::broadcast;
use url::Url;

/// Same-origin scope with a path component boundary (`/docs` excludes `/docs-old`).
#[derive(Clone, Debug, Serialize)]
pub struct CrawlScope {
    origin: Url,
    path_prefix: String,
}

impl CrawlScope {
    pub fn new(origin: Url, path_prefix: &str) -> Result<Self, CrawlError> {
        validate_url(&origin)?;
        if !path_prefix.starts_with('/')
            || path_prefix.contains(['?', '#', '%', '\\'])
            || path_prefix
                .split('/')
                .any(|part| matches!(part, "." | ".."))
        {
            return Err(CrawlError(
                "scope requires an absolute, unambiguous URL path".into(),
            ));
        }
        let path_prefix = path_prefix.trim_end_matches('/').to_owned();
        Ok(Self {
            origin,
            path_prefix,
        })
    }

    pub fn contains(&self, url: &Url) -> bool {
        let path = url.path();
        validate_url(url).is_ok()
            && self.origin.origin() == url.origin()
            // Encoded paths need a server-aware policy; reject them for now.
            && !path.contains('%')
            && (self.path_prefix.is_empty()
                || path == self.path_prefix
                || path
                    .strip_prefix(&self.path_prefix)
                    .is_some_and(|rest| rest.starts_with('/')))
    }
}

fn validate_url(url: &Url) -> Result<(), CrawlError> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(CrawlError(
            "crawl URLs must be HTTP(S) without credentials".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize)]
pub struct CrawlRequest {
    pub source_id: String,
    pub crawl_id: String,
    pub seed: Url,
    pub scope: CrawlScope,
    pub max_pages: u32,
    pub concurrency: usize,
    /// Absolute URL path segment count, not distance in the link graph.
    pub max_path_segments: Option<usize>,
    pub max_redirects: usize,
    pub request_timeout: Duration,
    /// Retained body limit; Spider's HTTP download limit is separate.
    pub max_retained_body_bytes: usize,
    pub subscription_capacity: usize,
    pub audit_capacity: usize,
    pub respect_robots: bool,
    /// Explicit opt-in for redirects within an intentionally local seed origin.
    /// Only IP loopback seeds qualify; default redirects retain Spider's guard.
    pub allow_loopback_redirects: bool,
}

impl CrawlRequest {
    pub fn new(
        source_id: impl Into<String>,
        crawl_id: impl Into<String>,
        seed: Url,
        scope: CrawlScope,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            crawl_id: crawl_id.into(),
            seed,
            scope,
            max_pages: 100,
            concurrency: 2,
            max_path_segments: None,
            max_redirects: 5,
            request_timeout: Duration::from_secs(20),
            max_retained_body_bytes: 2 * 1024 * 1024,
            subscription_capacity: 16,
            audit_capacity: 4096,
            respect_robots: true,
            allow_loopback_redirects: false,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CrawlError> {
        if self.source_id.is_empty()
            || self.crawl_id.is_empty()
            || !self.scope.contains(&self.seed)
            || !(1..=10_000).contains(&self.max_pages)
            || !(1..=64).contains(&self.concurrency)
            || !(1..=65_536).contains(&self.subscription_capacity)
            || !(1..=65_536).contains(&self.audit_capacity)
            || !(1..=16 * 1024 * 1024).contains(&self.max_retained_body_bytes)
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(120)
            || self.max_redirects > 20
            || self.max_path_segments == Some(0)
            || self
                .max_path_segments
                .is_some_and(|max| path_segments(&self.seed) > max)
        {
            return Err(CrawlError(
                "invalid crawl identity, scope, or resource limits".into(),
            ));
        }
        Ok(())
    }
}

fn path_segments(url: &Url) -> usize {
    url.path()
        .split('/')
        .filter(|part| !part.is_empty())
        .count()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageState {
    Fetched,
    HttpFailure,
    TransportFailure { message: String },
    Truncated,
    BodyTooLarge,
    Blocked,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PageOutcome {
    pub source_id: String,
    pub crawl_id: String,
    pub requested_url: String,
    pub final_url: String,
    pub fetched_at: SystemTime,
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    pub raw_body: Vec<u8>,
    pub content_truncated: bool,
    pub state: PageState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockReason {
    Robots,
    RedirectScope,
    RedirectLimit,
    RedirectPathDepth,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockedUrl {
    pub requested_url: String,
    pub target_url: String,
    pub reason: BlockReason,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrawlReport {
    pub source_id: String,
    pub crawl_id: String,
    pub started_at: SystemTime,
    pub finished_at: SystemTime,
    pub pages: Vec<PageOutcome>,
    pub blocked: Vec<BlockedUrl>,
    pub dropped_pages: u64,
    pub audit_overflow: bool,
}

impl CrawlReport {
    /// Delivery completeness only; budgets/robots can intentionally limit coverage.
    pub fn delivery_complete(&self) -> bool {
        self.dropped_pages == 0 && !self.audit_overflow
    }
}

#[derive(Debug)]
pub struct CrawlError(String);
impl fmt::Display for CrawlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Error for CrawlError {}

/// Collects a bounded run. Callers must check delivery completeness before ingestion.
pub async fn crawl(request: CrawlRequest) -> Result<CrawlReport, CrawlError> {
    request.validate()?;
    tracing::info!(source_id = %request.source_id, crawl_id = %request.crawl_id, seed = %request.seed, "starting crawl");
    let started_at = SystemTime::now();
    let mut seed = request.seed.clone();
    seed.set_fragment(None);
    let mut website = Website::new(seed.as_str());
    let origin = regex::escape(&request.scope.origin.origin().ascii_serialization());
    let prefix = regex::escape(&request.scope.path_prefix);
    website.with_whitelist_url(Some(vec![
        format!("^{origin}{prefix}(?:/[^%?#]*(?:\\?[^#]*)?|\\?[^#]*|$)$").into(),
    ]));
    website.with_limit(request.max_pages);
    website.with_concurrency_limit(Some(request.concurrency));
    website.with_respect_robots_txt(request.respect_robots);
    website.with_user_agent(Some("Ariadne/0.1"));
    website.with_retry(0);
    website.with_request_timeout(Some(request.request_timeout));
    // Spider adjusts depth relative to seed path; set the absolute cap directly.
    website.configuration.depth = 0;
    website.configuration.depth_distance = request.max_path_segments.unwrap_or(0);

    let (audit_tx, audit_rx) = mpsc::sync_channel(request.audit_capacity);
    let overflow = Arc::new(AtomicBool::new(false));
    let robots_tx = audit_tx.clone();
    let robots_overflow = Arc::clone(&overflow);
    website.with_on_link_blocked_callback(Some(move |url: String| {
        if robots_tx
            .try_send(BlockedUrl {
                requested_url: url.clone(),
                target_url: url,
                reason: BlockReason::Robots,
            })
            .is_err()
        {
            robots_overflow.store(true, Ordering::Relaxed);
        }
    }));

    let engine_policy = website.setup_strict_policy();
    let scope = request.scope.clone();
    let max_redirects = request.max_redirects;
    let depth = request.max_path_segments;
    let loopback = request.allow_loopback_redirects
        && match request.seed.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
    let redirect_overflow = Arc::clone(&overflow);
    let policy = spider::client::redirect::Policy::custom(move |attempt| {
        let target = attempt.url();
        let reason = if !scope.contains(target) {
            Some(BlockReason::RedirectScope)
        } else if attempt.previous().len() > max_redirects {
            Some(BlockReason::RedirectLimit)
        } else if depth.is_some_and(|max| path_segments(target) > max) {
            Some(BlockReason::RedirectPathDepth)
        } else {
            None
        };
        if let Some(reason) = reason {
            if audit_tx
                .try_send(BlockedUrl {
                    requested_url: attempt
                        .previous()
                        .first()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    target_url: target.to_string(),
                    reason,
                })
                .is_err()
            {
                redirect_overflow.store(true, Ordering::Relaxed);
            }
            return attempt.error("Ariadne redirect boundary blocked");
        }
        if loopback {
            attempt.follow()
        } else {
            engine_policy.redirect(attempt)
        }
    });
    let client = website
        .configure_http_client_builder()
        .no_proxy()
        .redirect(policy)
        .build()
        .map_err(|error| CrawlError(error.to_string()))?;
    website.set_http_client(client);
    let mut receiver = website.subscribe(request.subscription_capacity);
    let collect = async {
        let mut pages = Vec::new();
        let mut dropped_pages = 0;
        loop {
            match receiver.recv().await {
                Ok(page) => {
                    if pages.len() >= request.max_pages as usize {
                        dropped_pages += 1;
                    } else {
                        pages.push(page_outcome(page, &request));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(count, "crawler subscription lost pages");
                    dropped_pages += count;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        (pages, dropped_pages)
    };
    let run = async {
        website.crawl().await;
        website.unsubscribe();
    };
    let (_, (mut pages, dropped_pages)) = tokio::join!(run, collect);
    let mut blocked: Vec<_> = audit_rx.try_iter().collect();
    // Arrival order depends on network timing; expose deterministic result ordering.
    pages.sort_by(|a, b| a.requested_url.cmp(&b.requested_url));
    blocked
        .sort_by(|a, b| (&a.requested_url, &a.target_url).cmp(&(&b.requested_url, &b.target_url)));
    tracing::info!(
        pages = pages.len(),
        blocked = blocked.len(),
        dropped_pages,
        audit_overflow = overflow.load(Ordering::Relaxed),
        "finished crawl"
    );
    Ok(CrawlReport {
        source_id: request.source_id,
        crawl_id: request.crawl_id,
        started_at,
        finished_at: SystemTime::now(),
        pages,
        blocked,
        dropped_pages,
        audit_overflow: overflow.load(Ordering::Relaxed),
    })
}

fn page_outcome(page: Page, request: &CrawlRequest) -> PageOutcome {
    let body = page.get_html_bytes_u8();
    let state = if page.blocked_crawl {
        PageState::Blocked
    } else if let Some(error) = page
        .error_status
        .as_ref()
        .filter(|error| !error.is_status())
    {
        PageState::TransportFailure {
            message: error.to_string(),
        }
    } else if page.content_truncated {
        PageState::Truncated
    } else if body.len() > request.max_retained_body_bytes {
        PageState::BodyTooLarge
    } else if page.status_code.is_client_error() || page.status_code.is_server_error() {
        PageState::HttpFailure
    } else if page.status_code.is_success() {
        PageState::Fetched
    } else {
        PageState::HttpFailure
    };
    PageOutcome {
        source_id: request.source_id.clone(),
        crawl_id: request.crawl_id.clone(),
        requested_url: page.get_url().to_owned(),
        final_url: page
            .final_redirect_destination
            .clone()
            .unwrap_or_else(|| page.get_url().to_owned()),
        fetched_at: SystemTime::now(),
        status: page.status_code.as_u16(),
        headers: page
            .headers
            .as_ref()
            .map(|headers| {
                headers
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
                    .collect()
            })
            .unwrap_or_default(),
        raw_body: if body.len() <= request.max_retained_body_bytes {
            body.to_vec()
        } else {
            Vec::new()
        },
        content_truncated: page.content_truncated,
        state,
    }
}

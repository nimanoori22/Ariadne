//! Persisted validators supplied through Spider's per-URL fetch hook.
use super::{CrawlRequest, PageOutcome, PageState};
use async_trait::async_trait;
use spider::{
    client::{
        Client, StatusCode,
        header::{HeaderMap, HeaderValue},
    },
    fetch_engine::{EngineError, EngineRequest, EngineResponse, HttpFetchEngine},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

/// Storage implements this narrow boundary. Bodies are loaded on demand, never
/// preloaded for an entire documentation source.
#[async_trait]
pub trait PageCache: Send + Sync {
    fn urls(&self) -> &[String];
    async fn get(&self, url: &str) -> anyhow::Result<Option<PageOutcome>>;
}

pub(super) struct RevalidationEngine {
    pub client: Client,
    pub cache: Arc<dyn PageCache>,
    pub request: CrawlRequest,
    pub audit: mpsc::SyncSender<PageOutcome>,
    pub overflow: Arc<AtomicBool>,
}

fn validators(page: &PageOutcome) -> HeaderMap {
    let mut result = HeaderMap::new();
    // These are retained knowledge bytes, not an HTTP cache for arbitrary Vary
    // representations. Be conservative when reuse depends on request headers.
    if page.headers.iter().any(|(name, value)| {
        (name.eq_ignore_ascii_case("vary") && !value.is_empty())
            || (name.eq_ignore_ascii_case("cache-control")
                && String::from_utf8_lossy(value)
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("no-store")))
    }) {
        return result;
    }
    for (name, value) in &page.headers {
        let target = if name.eq_ignore_ascii_case("etag") {
            "if-none-match"
        } else if name.eq_ignore_ascii_case("last-modified") {
            "if-modified-since"
        } else {
            continue;
        };
        let valid = if target == "if-none-match" {
            let tag = value.strip_prefix(b"W/").unwrap_or(value);
            tag.len() >= 2
                && tag.first() == Some(&b'"')
                && tag.last() == Some(&b'"')
                && tag[1..tag.len() - 1]
                    .iter()
                    .all(|b| *b == 0x21 || (0x23..=0x7e).contains(b) || *b >= 0x80)
        } else {
            std::str::from_utf8(value)
                .ok()
                .is_some_and(|date| httpdate::parse_http_date(date).is_ok())
        };
        if value.len() <= 4096
            && valid
            && let Ok(value) = HeaderValue::from_bytes(value)
        {
            result.insert(target, value);
        }
    }
    result
}

#[async_trait]
impl HttpFetchEngine for RevalidationEngine {
    fn should_fetch(&self, url: &str) -> bool {
        self.cache
            .urls()
            .binary_search_by(|saved| saved.as_str().cmp(url))
            .is_ok()
    }

    async fn fetch(&self, req: EngineRequest<'_>) -> Result<EngineResponse, EngineError> {
        let saved = self
            .cache
            .get(req.url)
            .await
            .map_err(|_| EngineError::Other("read persisted page cache".into()))?;
        let saved = saved.filter(|page| {
            page.state == PageState::Fetched
                && page.status == 200
                && !page.content_truncated
                && !page.raw_body.is_empty()
                && page.raw_body.len() <= self.request.max_retained_body_bytes
                && page.final_url == req.url
                && page.requested_url == req.url
        });
        let conditional = saved.as_ref().map(validators).unwrap_or_default();
        let mut response = self
            .client
            .get(req.url)
            .headers(conditional.clone())
            .send()
            .await
            .map_err(transport_error)?;
        // A validator belongs to one representation URL. If a redirect target
        // answers 304, fetch without conditions rather than reuse another body.
        if response.status() == StatusCode::NOT_MODIFIED && response.url().as_str() != req.url {
            response = self
                .client
                .get(req.url)
                .send()
                .await
                .map_err(transport_error)?;
        }
        let status_code = response.status();
        let final_url = (response.url().as_str() != req.url).then(|| response.url().to_string());
        let headers = response.headers().clone();
        if status_code == StatusCode::NOT_MODIFIED && !conditional.is_empty() && final_url.is_none()
        {
            let cached = saved.expect("conditional headers require cached content");
            let page = PageOutcome {
                source_id: self.request.source_id.clone(),
                crawl_id: self.request.crawl_id.clone(),
                requested_url: req.url.into(),
                final_url: req.url.into(),
                fetched_at: std::time::SystemTime::now(),
                status: 304,
                headers: headers
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                    .collect(),
                raw_body: vec![],
                content_truncated: false,
                state: PageState::NotModified,
            };
            if self.audit.try_send(page).is_err() {
                self.overflow.store(true, Ordering::Relaxed);
                return Err(EngineError::Other(
                    "revalidation audit capacity exceeded".into(),
                ));
            }
            let mut discovery_headers = HeaderMap::new();
            for (key, value) in &cached.headers {
                if let (Ok(key), Ok(value)) = (
                    key.parse::<spider::client::header::HeaderName>(),
                    HeaderValue::from_bytes(value),
                ) {
                    discovery_headers.insert(key, value);
                }
            }
            discovery_headers.remove("content-length");
            return Ok(EngineResponse {
                status_code: StatusCode::OK,
                final_url: None,
                headers: discovery_headers,
                body: cached.raw_body,
                served: true,
                ..Default::default()
            });
        }
        // Bound retained response memory. One extra byte lets the existing
        // adapter classify oversized bodies explicitly instead of indexing them.
        let mut body = Vec::new();
        let cap = self.request.max_retained_body_bytes + 1;
        while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
            let remaining = cap.saturating_sub(body.len());
            body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            if body.len() == cap {
                break;
            }
        }
        Ok(EngineResponse {
            status_code,
            final_url,
            headers,
            body,
            served: true,
            ..Default::default()
        })
    }
}

fn transport_error(error: spider::client::Error) -> EngineError {
    if error.is_timeout() {
        EngineError::Timeout
    } else if error.is_connect() {
        EngineError::ConnectAborted
    } else {
        EngineError::Other("conditional fetch transport failure".into())
    }
}

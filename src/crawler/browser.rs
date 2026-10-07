//! Lazy Chromium rendering using Spider's browser API and guarded HTTP transport.
//! Chromium's own transport is confined to a local rejecting proxy. Only our
//! interception handler performs origin requests through the crawler's client.
use super::{CrawlRequest, PageOutcome, PageState};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use spider::{
    chromiumoxide::{
        browser::{Browser, BrowserConfig},
        cdp::browser_protocol::{
            fetch::{EventRequestPaused, FulfillRequestParams, HeaderEntry},
            network::ResourceType,
        },
    },
    client::Client,
    fetch_engine::EngineResponse,
    packages::robotparser::parser::RobotFileParser,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{io::AsyncWriteExt, net::TcpListener, task::JoinHandle};
use url::Url;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderMetadata {
    pub version: String,
    pub outcome: String,
    pub attempted_at: SystemTime,
    pub elapsed_ms: u64,
    pub original_body: Option<Vec<u8>>,
    pub blocked_urls: Vec<String>,
    pub resource_requests: usize,
    pub resource_bytes: usize,
    pub direct_requests_blocked: usize,
}
struct Task(JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Session {
    browser: Browser,
    _handler: Task,
    _proxy: Task,
    _profile: tempfile::TempDir,
    direct: Arc<AtomicUsize>,
}
impl Session {
    async fn launch() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let proxy = listener.local_addr()?;
        let direct = Arc::new(AtomicUsize::new(0));
        let attempts = direct.clone();
        let proxy_task = Task(tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                attempts.fetch_add(1, Ordering::Relaxed);
                let _ = tokio::time::timeout(
                    Duration::from_secs(1),
                    stream.write_all(
                        b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    ),
                )
                .await;
            }
        }));
        let executable = std::env::var("ARIADNE_CHROMIUM_PATH")
            .ok()
            .or_else(|| {
                let path = std::env::var_os("PATH")?;
                std::env::split_paths(&path)
                    .flat_map(|directory| {
                        ["chromium", "chromium-browser", "google-chrome", "chrome"].map(|name| {
                            directory.join(if cfg!(windows) {
                                format!("{name}.exe")
                            } else {
                                name.to_owned()
                            })
                        })
                    })
                    .find(|p| p.is_file())
                    .map(|p| p.to_string_lossy().into_owned())
            })
            .context("Chromium unavailable; set ARIADNE_CHROMIUM_PATH")?;
        let profile = tempfile::tempdir()?;
        let config = BrowserConfig::builder()
            .chrome_executable(executable)
            .user_data_dir(profile.path())
            .enable_request_intercept()
            .disable_cache()
            .launch_timeout(Duration::from_secs(10))
            .request_timeout(Duration::from_secs(5))
            .args([
                format!("--proxy-server=http://{proxy}"),
                "--proxy-bypass-list=<-loopback>".into(),
                "--host-resolver-rules=MAP * ~NOTFOUND".into(),
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp".into(),
                "--disable-background-networking".into(),
                "--disable-extensions".into(),
                "--disable-quic".into(),
                "--disable-features=ServiceWorker,SharedWorker".into(),
                "--js-flags=--max-old-space-size=128".into(),
            ])
            .build()
            .map_err(anyhow::Error::msg)?;
        // Keep Chromium's sandbox enabled. No remote browser or persistent user
        // profile is accepted: source JavaScript never receives user sessions.
        let (browser, mut handler) = Browser::launch(config).await?;
        let handler = Task(tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                if event.is_err() {
                    break;
                }
            }
        }));
        Ok(Self {
            browser,
            _handler: handler,
            _proxy: proxy_task,
            _profile: profile,
            direct,
        })
    }
}
#[derive(Default)]
pub(super) struct Renderer {
    session: tokio::sync::Mutex<Option<Session>>,
}
#[derive(Default)]
struct Metrics {
    blocked: Vec<String>,
    requests: usize,
    bytes: usize,
}
impl Renderer {
    pub async fn shutdown(&self) {
        let mut session = self.session.lock().await;
        if let Some(mut session) = session.take() {
            let _ = session.browser.kill().await;
        }
    }
    pub async fn upgrade(
        &self,
        client: &Client,
        request: &CrawlRequest,
        url: &str,
        response: &mut EngineResponse,
        robots: Option<&RobotFileParser>,
    ) -> Option<RenderMetadata> {
        let original = PageOutcome {
            source_id: request.source_id.clone(),
            crawl_id: request.crawl_id.clone(),
            requested_url: url.into(),
            final_url: response.final_url.clone().unwrap_or_else(|| url.into()),
            fetched_at: SystemTime::now(),
            status: response.status_code.as_u16(),
            headers: response
                .headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                .collect(),
            raw_body: std::mem::take(&mut response.body),
            content_truncated: false,
            rendering: None,
            state: PageState::Fetched,
        };
        if !crate::extraction::browser_candidate(&original)
            || original.raw_body.len() > request.max_retained_body_bytes
        {
            response.body = original.raw_body;
            return None;
        }
        let start = Instant::now();
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        let mut metadata = RenderMetadata {
            version: "guarded-chromium-v1".into(),
            outcome: "failed".into(),
            attempted_at: SystemTime::now(),
            elapsed_ms: 0,
            original_body: None,
            blocked_urls: vec![],
            resource_requests: 0,
            resource_bytes: 0,
            direct_requests_blocked: 0,
        };
        let mut session = self.session.lock().await;
        let result = tokio::time::timeout(request.request_timeout, async {
            if session.is_none() {
                *session = Some(Box::pin(Session::launch()).await?);
            }
            Box::pin(render(
                session.as_ref().unwrap(),
                client,
                request,
                &original,
                robots,
                metrics.clone(),
            ))
            .await
        })
        .await;
        match result {
            Ok(Ok(dom)) => {
                metadata.outcome = "rendered".into();
                metadata.original_body = Some(original.raw_body);
                response.body = dom;
                response.declared_content_length = None;
                response.headers.remove("content-length");
            }
            error => {
                tracing::warn!(url,error=?error,"browser fallback failed");
                response.body = original.raw_body;
                if let Some(mut browser) = session.take() {
                    metadata.direct_requests_blocked = browser.direct.load(Ordering::Relaxed);
                    let _ = browser.browser.kill().await;
                }
            }
        }
        if let Some(session) = session.as_ref() {
            metadata.direct_requests_blocked = session.direct.load(Ordering::Relaxed);
        }
        metadata.elapsed_ms = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let m = metrics.lock().unwrap();
        metadata.blocked_urls = m.blocked.clone();
        metadata.resource_requests = m.requests;
        metadata.resource_bytes = m.bytes;
        Some(metadata)
    }
}
async fn render(
    session: &Session,
    client: &Client,
    request: &CrawlRequest,
    original: &PageOutcome,
    robots: Option<&RobotFileParser>,
    metrics: Arc<Mutex<Metrics>>,
) -> Result<Vec<u8>> {
    let page = session.browser.new_page("about:blank").await?;
    let mut events = page.event_listener::<EventRequestPaused>().await?;
    let control = page.clone();
    let target = Url::parse(&original.final_url)?;
    let seed = target.clone();
    let html = original.raw_body.clone();
    let headers = original.headers.clone();
    let client = client.clone();
    let dom_limit = request.max_retained_body_bytes;
    let request = request.clone();
    let robots = robots.cloned();
    let tally = metrics.clone();
    let interception = Task(tokio::spawn(async move {
        while let Some(event) = events.next().await {
            let parsed = Url::parse(&event.request.url).ok();
            let count = {
                let mut m = tally.lock().unwrap();
                m.requests += 1;
                m.requests
            };
            let allowed = count <= 64
                && matches!(
                    event.resource_type,
                    ResourceType::Document
                        | ResourceType::Script
                        | ResourceType::Stylesheet
                        | ResourceType::Fetch
                        | ResourceType::Xhr
                )
                && event.request.method == "GET"
                && parsed.as_ref().is_some_and(|u| {
                    request.scope.contains(u)
                        && (!request.respect_robots
                            || robots
                                .as_ref()
                                .is_none_or(|r| r.can_fetch("Ariadne/0.1", u.as_str())))
                        && (event.resource_type != ResourceType::Document
                            || u.as_str() == seed.as_str())
                });
            let fetched = async {
                ensure!(allowed, "blocked browser resource");
                if event.resource_type == ResourceType::Document {
                    return Ok((200, headers.clone(), html.clone()));
                }
                let mut reply = client.get(parsed.unwrap()).send().await?;
                let code = reply.status().as_u16();
                let headers = reply
                    .headers()
                    .iter()
                    .filter(|(k, _)| {
                        !matches!(
                            k.as_str(),
                            "content-encoding" | "content-length" | "set-cookie"
                        )
                    })
                    .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                    .collect();
                let mut body = Vec::new();
                while let Some(bytes) = reply.chunk().await? {
                    ensure!(
                        body.len() + bytes.len() <= request.max_retained_body_bytes,
                        "browser resource too large"
                    );
                    body.extend_from_slice(&bytes);
                }
                let mut m = tally.lock().unwrap();
                ensure!(
                    m.bytes + body.len() <= 8 * 1024 * 1024,
                    "browser resource budget exceeded"
                );
                m.bytes += body.len();
                Ok::<_, anyhow::Error>((code, headers, body))
            }
            .await;
            let (code, headers, body) = match fetched {
                Ok(value) => value,
                Err(_) => {
                    let mut m = tally.lock().unwrap();
                    if m.blocked.len() < 64 {
                        m.blocked
                            .push(event.request.url.chars().take(4096).collect());
                    }
                    (403, vec![], vec![])
                }
            };
            let headers: Vec<_> = headers
                .into_iter()
                .filter(|(k, _)| {
                    !matches!(
                        k.as_str(),
                        "content-encoding" | "content-length" | "set-cookie"
                    )
                })
                .map(|(k, v)| HeaderEntry::new(k, String::from_utf8_lossy(&v).into_owned()))
                .collect();
            if let Ok(command) = FulfillRequestParams::builder()
                .request_id(event.request_id.clone())
                .response_code(code)
                .response_headers(headers)
                .body(STANDARD.encode(body))
                .build()
                && control.execute(command).await.is_err()
            {
                break;
            }
        }
    }));
    let result=async {
        page.goto(target.as_str()).await?;
        let mut stable=None;
        loop {
            let actual=page.url().await?.context("browser has no document URL")?;let mut actual=Url::parse(&actual)?;actual.set_fragment(None);ensure!(actual==target,"browser navigation changed provenance");
            let expression=format!("(() => {{const h=document.documentElement.outerHTML;return new TextEncoder().encode(h).length<={} ? h : null;}})()",dom_limit);
            let html:Option<String>=page.evaluate(expression).await?.into_value()?;let html=html.context("rendered DOM exceeds body budget")?;
            let candidate=PageOutcome{raw_body:html.as_bytes().to_vec(),..original.clone()};
            if !crate::extraction::browser_candidate(&candidate) && matches!(crate::extraction::extract(candidate),crate::extraction::ExtractionOutcome::Extracted(_)) {
                if stable.as_ref()==Some(&html){return Ok(html.into_bytes());}stable=Some(html);
            }else{stable=None;}
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }.await;
    drop(interception);
    let _ = page.close().await;
    result
}

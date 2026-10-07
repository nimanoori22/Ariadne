//! Local integration probes for the crawler engine, before building an adapter.
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use spider::{page::Page, website::Website};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, broadcast},
    task::JoinHandle,
    time::timeout,
};

struct Fixture {
    root: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fixture {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}", listener.local_addr().unwrap());
        let host_redirect = format!(
            "Location: {}/outside\r\n",
            root.replace("127.0.0.1", "localhost")
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                // Tiny sequential HTTP fixture; responses always close the connection.
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0; 1024];
                    let count = stream.read(&mut bytes).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    request.extend_from_slice(&bytes[..count]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                    assert!(request.len() < 16384, "oversized fixture request");
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split_whitespace().nth(1).unwrap_or("/");
                recorded.lock().await.push(path.to_owned());
                if path == "/docs/disconnect" {
                    // A deterministic transport failure: close without an HTTP response.
                    continue;
                }
                if path == "/docs/slow" {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let (status, headers, body) = match path {
                    "/docs/slow" => ("200 OK", "", "<html><body>Slow fixture</body></html>"),
                    "/docs/many-blocked" => (
                        "200 OK",
                        "",
                        "<html><body><a href='/docs/blocked/one'>One</a><a href='/docs/blocked/two'>Two</a><a href='/docs/blocked/three'>Three</a></body></html>",
                    ),
                    "/docs/chain" => ("302 Found", "Location: /docs/path-escape\r\n", ""),
                    "/docs/loop" => ("302 Found", "Location: /docs/loop\r\n", ""),
                    "/robots.txt" => ("200 OK", "", "User-agent: *\nDisallow: /docs/blocked\n"),
                    "/docs/robots" => (
                        "200 OK",
                        "",
                        "<html><body><a href='/docs/blocked'>Blocked</a><a href='/docs/allowed'>Allowed</a></body></html>",
                    ),
                    "/docs/allowed" | "/docs/blocked" => {
                        ("200 OK", "", "<html><body>Robots fixture</body></html>")
                    }
                    "/docs/depth" => (
                        "200 OK",
                        "",
                        "<html><body><a href='/docs/hop1'>First hop</a><a href='/docs/depth/child'>Nested child</a></body></html>",
                    ),
                    "/docs/hop1" => (
                        "200 OK",
                        "",
                        "<html><body><a href='/docs/hop2'>Second hop</a></body></html>",
                    ),
                    "/docs/hop2" => (
                        "200 OK",
                        "",
                        "<html><body><a href='/docs/hop3'>Third hop</a></body></html>",
                    ),
                    "/docs/hop3" | "/docs/depth/child" => {
                        ("200 OK", "", "<html><body>Depth fixture</body></html>")
                    }
                    "/docs/errors" => (
                        "200 OK",
                        "",
                        "<html><body><a href='/docs/missing'>Missing</a><a href='/docs/unavailable'>Unavailable</a><a href='/docs/disconnect'>Disconnect</a></body></html>",
                    ),
                    "/docs/missing" => ("404 Not Found", "", "Missing document"),
                    "/docs/unavailable" => (
                        "503 Service Unavailable",
                        "Retry-After: 1\r\n",
                        "Temporarily unavailable",
                    ),
                    "/docs/path-escape" => ("302 Found", "Location: /outside\r\n", ""),
                    "/docs/host-escape" => ("302 Found", host_redirect.as_str(), ""),
                    "/docs/" => (
                        "200 OK",
                        "",
                        r#"<!doctype html><html><body><main>
                        <h1>Fixture documentation</h1>
                        <a href="/docs/api">API</a><a href="/docs/api#example">Duplicate</a>
                        <a href="/docs/redirect">Redirect</a><a href="/outside">Outside</a>
                        </main></body></html>"#,
                    ),
                    "/docs/api" => (
                        "200 OK",
                        "",
                        r#"<!doctype html><html><body><nav>Menu</nav><main>
                        <h1>Client</h1><h2 id="example">Proxy example</h2>
                        <pre><code class="language-rust">let proxy = Proxy::custom();
    // preserve indentation</code></pre>
                        <table><tr><th>Option</th></tr><tr><td>SOCKS</td></tr></table>
                        </main></body></html>"#,
                    ),
                    "/docs/redirect" => ("302 Found", "Location: /docs/target\r\n", ""),
                    "/docs/target" => (
                        "200 OK",
                        "",
                        "<html><body><h1>Redirect target</h1></body></html>",
                    ),
                    "/outside" => ("200 OK", "", "<html><body>Outside scope</body></html>"),
                    _ => ("404 Not Found", "", "Missing"),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}",
                    body.len()
                );
                // A timed-out crawler may disconnect before the response is written.
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        Self {
            root,
            requests,
            task,
        }
    }

    fn crawler(&self) -> Website {
        self.crawler_at("/docs/")
    }

    fn crawler_at(&self, path: &str) -> Website {
        let mut crawler = Website::new(&format!("{}{path}", self.root));
        // The fixture uses an IPv4 literal; escape its dots in the URL regex.
        let escaped_root = self.root.replace('.', r"\.");
        crawler.with_whitelist_url(Some(vec![format!("^{escaped_root}/docs/.*$").into()]));
        crawler.with_limit(10);
        crawler.with_concurrency_limit(Some(2));
        crawler.with_retry(0);
        crawler
    }

    async fn requested(&self, path: &str) -> bool {
        self.requests
            .lock()
            .await
            .iter()
            .any(|requested| requested == path)
    }
}

async fn crawl(mut crawler: Website) -> Vec<Page> {
    let mut receiver = crawler.subscribe(16);
    let consumer = tokio::spawn(async move {
        let mut pages = Vec::new();
        loop {
            match receiver.recv().await {
                Ok(page) => pages.push(page),
                Err(broadcast::error::RecvError::Closed) => return pages,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    panic!("crawler dropped {count} pages");
                }
            }
        }
    });
    timeout(Duration::from_secs(15), Box::pin(crawler.crawl_raw()))
        .await
        .expect("crawl timed out");
    crawler.unsubscribe();
    timeout(Duration::from_secs(5), consumer)
        .await
        .expect("subscription did not close")
        .unwrap()
}

#[tokio::test]
async fn discovers_pages_preserves_raw_content_and_obeys_path_scope() {
    let fixture = Fixture::start().await;
    let pages = crawl(fixture.crawler()).await;
    let urls: BTreeSet<_> = pages.iter().map(|page| page.get_url().to_owned()).collect();
    assert!(urls.contains(&format!("{}/docs/", fixture.root)));
    assert!(urls.contains(&format!("{}/docs/api", fixture.root)));
    assert_eq!(
        pages
            .iter()
            .filter(|page| page.get_url().ends_with("/docs/api"))
            .count(),
        1
    );
    let api = pages
        .iter()
        .find(|page| page.get_url().ends_with("/docs/api"))
        .unwrap();
    let html = api.get_html();
    assert!(html.contains("<h2 id=\"example\">"));
    assert!(html.contains("\n    // preserve indentation"));
    assert!(html.contains("<table>"));
    assert!(
        !fixture
            .requests
            .lock()
            .await
            .iter()
            .any(|path| path == "/outside")
    );
    assert!(
        fixture
            .requests
            .lock()
            .await
            .iter()
            .any(|path| path == "/docs/redirect")
    );
    // Spider's default redirect policy refuses loopback redirect destinations,
    // even when the explicitly configured seed itself is on loopback.
    assert!(
        !fixture
            .requests
            .lock()
            .await
            .iter()
            .any(|path| path == "/docs/target")
    );
}

#[tokio::test]
async fn page_budget_limits_fetches() {
    let fixture = Fixture::start().await;
    let mut crawler = fixture.crawler();
    crawler.with_limit(1);
    let pages = crawl(crawler).await;
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].get_url(), format!("{}/docs/", fixture.root));
    assert_eq!(*fixture.requests.lock().await, vec!["/docs/"]);
}

#[tokio::test]
async fn undersized_subscription_reports_lag_instead_of_silent_success() {
    let fixture = Fixture::start().await;
    let mut crawler = fixture.crawler();
    let mut receiver = crawler.subscribe(1);
    // Deliberately leave the consumer idle until crawling completes.
    timeout(Duration::from_secs(15), crawler.crawl())
        .await
        .unwrap();
    crawler.unsubscribe();
    assert!(matches!(
        receiver.recv().await,
        Err(broadcast::error::RecvError::Lagged(_))
    ));
}

#[tokio::test]
async fn depth_limits_url_path_segments_rather_than_link_hops() {
    let fixture = Fixture::start().await;
    let mut crawler = fixture.crawler_at("/docs/depth");
    crawler.with_depth(2);
    let pages = crawl(crawler).await;
    assert!(
        pages
            .iter()
            .any(|page| page.get_url().ends_with("/docs/hop3"))
    );
    assert!(!fixture.requested("/docs/depth/child").await);

    let mut crawler = fixture.crawler_at("/docs/depth");
    crawler.with_depth(3);
    crawl(crawler).await;
    assert!(fixture.requested("/docs/depth/child").await);
}

#[tokio::test]
async fn robots_rules_block_disallowed_links_when_enabled() {
    let fixture = Fixture::start().await;
    let mut crawler = fixture.crawler_at("/docs/robots");
    crawler.with_respect_robots_txt(true);
    crawler.with_user_agent(Some("AriadneFixture"));
    let pages = crawl(crawler).await;
    assert!(fixture.requested("/robots.txt").await);
    assert!(fixture.requested("/docs/allowed").await);
    assert!(!fixture.requested("/docs/blocked").await);
    assert!(
        !pages
            .iter()
            .any(|page| page.get_url().ends_with("/docs/blocked"))
    );

    // Control: the blocked page is reachable when robots handling is disabled.
    crawl(fixture.crawler_at("/docs/robots")).await;
    assert!(fixture.requested("/docs/blocked").await);
}

#[tokio::test]
async fn http_and_transport_failures_are_delivered_to_subscribers() {
    let fixture = Fixture::start().await;
    let pages = crawl(fixture.crawler_at("/docs/errors")).await;
    let page_at = |path: &str| {
        pages
            .iter()
            .find(|page| page.get_url() == format!("{}{path}", fixture.root))
            .unwrap()
    };
    assert_eq!(page_at("/docs/missing").status_code.as_u16(), 404);
    let unavailable = page_at("/docs/unavailable");
    assert_eq!(unavailable.status_code.as_u16(), 503);
    assert_eq!(
        unavailable
            .headers
            .as_ref()
            .unwrap()
            .get("retry-after")
            .unwrap(),
        "1"
    );
    assert!(
        page_at("/docs/disconnect")
            .error_status
            .as_ref()
            .is_some_and(|error| !error.to_string().is_empty())
    );
    for path in ["/docs/missing", "/docs/unavailable"] {
        assert_eq!(
            fixture
                .requests
                .lock()
                .await
                .iter()
                .filter(|requested| requested.as_str() == path)
                .count(),
            1
        );
    }
}

/// A test-only client permits local redirects so scope behavior can be tested
/// independently of Spider's default loopback redirect guard. Never install
/// this permissive policy in a production adapter.
fn permit_fixture_redirects(crawler: &mut Website) {
    let client = crawler
        .configure_http_client_builder()
        .no_proxy()
        .redirect(spider::client::redirect::Policy::limited(5))
        .build()
        .unwrap();
    crawler.set_http_client(client);
}

#[tokio::test]
async fn allowed_redirect_delivers_content_and_destination_metadata() {
    let fixture = Fixture::start().await;
    let mut crawler = fixture.crawler_at("/docs/redirect");
    permit_fixture_redirects(&mut crawler);
    let pages = crawl(crawler).await;
    assert!(fixture.requested("/docs/target").await);
    assert_eq!(pages.len(), 1);
    assert_eq!(
        pages[0].get_url(),
        format!("{}/docs/redirect", fixture.root)
    );
    assert_eq!(pages[0].status_code.as_u16(), 200);
    assert!(pages[0].get_html().contains("Redirect target"));
    assert_eq!(
        pages[0].final_redirect_destination.as_deref(),
        Some(format!("{}/docs/target", fixture.root).as_str())
    );
}

#[tokio::test]
async fn discovery_whitelist_does_not_replace_a_redirect_policy() {
    for path in ["/docs/path-escape", "/docs/host-escape"] {
        let fixture = Fixture::start().await;
        let mut crawler = fixture.crawler_at(path);
        permit_fixture_redirects(&mut crawler);
        let pages = crawl(crawler).await;
        // Characterize the gap rather than assuming discovery filters apply
        // inside a user-supplied HTTP client's redirect chain.
        assert!(fixture.requested("/outside").await);
        assert!(
            pages
                .iter()
                .any(|page| page.get_html().contains("Outside scope"))
        );
    }
}

fn adapter_request(fixture: &Fixture, path: &str) -> ariadne::crawler::CrawlRequest {
    let seed = format!("{}{path}", fixture.root).parse().unwrap();
    let scope = ariadne::crawler::CrawlScope::new(fixture.root.parse().unwrap(), "/docs").unwrap();
    ariadne::crawler::CrawlRequest::new("fixture-source", "fixture-run", seed, scope)
}

#[tokio::test]
async fn adapter_preserves_provenance_and_allowed_redirects() {
    use ariadne::crawler::{PageState, crawl};
    let fixture = Fixture::start().await;
    let mut request = adapter_request(&fixture, "/docs/redirect");
    request.allow_loopback_redirects = true;
    let report = crawl(request).await.unwrap();
    assert!(report.delivery_complete());
    assert_eq!(report.pages.len(), 1);
    let page = &report.pages[0];
    assert_eq!(page.state, PageState::Fetched);
    assert_eq!(page.source_id, "fixture-source");
    assert_eq!(page.crawl_id, "fixture-run");
    assert_eq!(
        page.requested_url,
        format!("{}/docs/redirect", fixture.root)
    );
    assert_eq!(page.final_url, format!("{}/docs/target", fixture.root));
    assert!(String::from_utf8_lossy(&page.raw_body).contains("Redirect target"));
    assert!(page.fetched_at >= report.started_at && page.fetched_at <= report.finished_at);
}

#[tokio::test]
async fn adapter_blocks_scope_escapes_before_fetch_including_multi_hop() {
    use ariadne::crawler::{BlockReason, PageState, crawl};
    for path in ["/docs/path-escape", "/docs/host-escape", "/docs/chain"] {
        let fixture = Fixture::start().await;
        let mut request = adapter_request(&fixture, path);
        request.allow_loopback_redirects = true;
        let report = crawl(request).await.unwrap();
        assert!(report.delivery_complete());
        assert!(!fixture.requested("/outside").await);
        assert_eq!(report.blocked.len(), 1);
        assert_eq!(report.blocked[0].reason, BlockReason::RedirectScope);
        assert_eq!(
            report.blocked[0].requested_url,
            format!("{}{path}", fixture.root)
        );
        assert!(
            report
                .pages
                .iter()
                .all(|page| page.state != PageState::Fetched)
        );
    }
}

#[tokio::test]
async fn adapter_retains_default_loopback_guard_without_opt_in() {
    use ariadne::crawler::{PageState, crawl};
    let fixture = Fixture::start().await;
    let report = crawl(adapter_request(&fixture, "/docs/redirect"))
        .await
        .unwrap();
    assert!(!fixture.requested("/docs/target").await);
    assert!(matches!(
        report.pages[0].state,
        PageState::TransportFailure { .. }
    ));
}

#[tokio::test]
async fn adapter_enforces_redirect_cap_and_reports_loops() {
    use ariadne::crawler::{BlockReason, crawl};
    for (path, max_redirects) in [("/docs/redirect", 0), ("/docs/loop", 2)] {
        let fixture = Fixture::start().await;
        let mut request = adapter_request(&fixture, path);
        request.allow_loopback_redirects = true;
        request.max_redirects = max_redirects;
        let report = crawl(request).await.unwrap();
        assert_eq!(report.blocked[0].reason, BlockReason::RedirectLimit);
        assert!(!fixture.requested("/docs/target").await);
        let count = fixture
            .requests
            .lock()
            .await
            .iter()
            .filter(|url| url.as_str() == path)
            .count();
        assert_eq!(count, max_redirects + 1);
    }
}

#[tokio::test]
async fn adapter_classifies_http_and_transport_failures() {
    use ariadne::crawler::{PageState, crawl};
    let fixture = Fixture::start().await;
    let report = crawl(adapter_request(&fixture, "/docs/errors"))
        .await
        .unwrap();
    assert!(report.delivery_complete());
    assert_eq!(report.pages.len(), 4);
    for (path, status) in [("/docs/missing", 404), ("/docs/unavailable", 503)] {
        let page = report
            .pages
            .iter()
            .find(|page| page.requested_url.ends_with(path))
            .unwrap();
        assert_eq!(page.state, PageState::HttpFailure);
        assert_eq!(page.status, status);
    }
    let unavailable = report.pages.iter().find(|page| page.status == 503).unwrap();
    assert!(
        unavailable
            .headers
            .iter()
            .any(|(name, value)| name == "retry-after" && value == b"1")
    );
    let disconnected = report
        .pages
        .iter()
        .find(|page| page.requested_url.ends_with("/disconnect"))
        .unwrap();
    assert!(matches!(
        disconnected.state,
        PageState::TransportFailure { .. }
    ));
}

#[tokio::test]
async fn adapter_audits_robots_blocks_and_marks_overflow_incomplete() {
    use ariadne::crawler::{BlockReason, crawl};
    let fixture = Fixture::start().await;
    let report = crawl(adapter_request(&fixture, "/docs/robots"))
        .await
        .unwrap();
    assert!(report.delivery_complete());
    assert_eq!(report.blocked[0].reason, BlockReason::Robots);
    assert!(!fixture.requested("/docs/blocked").await);

    let mut request = adapter_request(&fixture, "/docs/many-blocked");
    request.audit_capacity = 1;
    let report = crawl(request).await.unwrap();
    assert!(report.audit_overflow);
    assert!(!report.delivery_complete());
    assert_eq!(report.blocked.len(), 1);
}

#[tokio::test]
async fn adapter_enforces_absolute_path_depth_and_retained_body_limit() {
    use ariadne::crawler::{PageState, crawl};
    let fixture = Fixture::start().await;
    let mut request = adapter_request(&fixture, "/docs/depth");
    request.max_path_segments = Some(2);
    let report = crawl(request).await.unwrap();
    assert!(
        report
            .pages
            .iter()
            .any(|page| page.requested_url.ends_with("/docs/hop3"))
    );
    assert!(!fixture.requested("/docs/depth/child").await);

    let mut request = adapter_request(&fixture, "/docs/api");
    request.max_retained_body_bytes = 16;
    let report = crawl(request).await.unwrap();
    assert_eq!(report.pages[0].state, PageState::BodyTooLarge);
    assert!(report.pages[0].raw_body.is_empty());
}

#[tokio::test]
async fn adapter_applies_request_timeout() {
    use ariadne::crawler::{PageState, crawl};
    let fixture = Fixture::start().await;
    let mut request = adapter_request(&fixture, "/docs/slow");
    request.respect_robots = false;
    request.request_timeout = Duration::from_millis(20);
    let report = timeout(Duration::from_secs(2), crawl(request))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        report.pages[0].state,
        PageState::TransportFailure { .. }
    ));
}

#[tokio::test]
async fn adapter_rejects_invalid_requests_without_fetching() {
    use ariadne::crawler::crawl;
    let fixture = Fixture::start().await;
    let mut request = adapter_request(&fixture, "/outside");
    assert!(crawl(request.clone()).await.is_err());
    request.seed = format!("{}/docs/", fixture.root).parse().unwrap();
    request.max_pages = 0;
    assert!(crawl(request).await.is_err());
    assert!(fixture.requests.lock().await.is_empty());
}

#[test]
fn scope_checks_origin_component_boundaries_and_ambiguous_paths() {
    use ariadne::crawler::CrawlScope;
    let scope = CrawlScope::new("https://example.com".parse().unwrap(), "/docs/").unwrap();
    for url in [
        "https://example.com/docs",
        "https://example.com/docs/api?version=1",
        "https://example.com/docs/api#method",
    ] {
        assert!(scope.contains(&url.parse().unwrap()), "{url}");
    }
    for url in [
        "https://example.com/docs-old",
        "http://example.com/docs",
        "https://example.com:8443/docs",
        "https://other.example.com/docs",
        "https://user@example.com/docs",
        "https://example.com/docs/%2foutside",
        "https://example.com/docs/%252foutside",
    ] {
        assert!(!scope.contains(&url.parse().unwrap()), "{url}");
    }
    assert!(CrawlScope::new("file:///tmp/docs".parse().unwrap(), "/docs").is_err());
    assert!(CrawlScope::new("https://example.com".parse().unwrap(), "/docs/../outside").is_err());
}

#[tokio::test]
async fn crawl_to_extraction_preserves_sections_code_and_failed_pages() {
    use ariadne::{
        crawler::crawl,
        extraction::{ContentBlock, ExtractionOutcome},
        ingestion::extract_crawl,
    };
    let fixture = Fixture::start().await;
    let batch = extract_crawl(crawl(adapter_request(&fixture, "/docs/api")).await.unwrap());
    assert!(batch.delivery_complete());
    let ExtractionOutcome::Extracted(document) = &batch.outcomes[0] else {
        panic!("expected extracted API page")
    };
    assert_eq!(document.title, "Client");
    assert_eq!(document.sections[2].parent_id, Some(1));
    assert_eq!(document.sections[2].anchor.as_deref(), Some("example"));
    assert!(
        matches!(&document.sections[2].blocks[0], ContentBlock::Code { text, .. } if text.contains("\n    // preserve indentation"))
    );
    assert!(document.markdown().contains("SOCKS"));
    assert_eq!(document.page.source_id, "fixture-source");

    let mut report = crawl(adapter_request(&fixture, "/docs/errors"))
        .await
        .unwrap();
    report.dropped_pages = 1;
    let batch = extract_crawl(report);
    assert!(!batch.delivery_complete());
    assert_eq!(batch.outcomes.len(), 4);
    assert_eq!(
        batch
            .outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ExtractionOutcome::Rejected { .. }))
            .count(),
        3
    );
}

#[tokio::test]
async fn registered_fixture_crawl_survives_embedded_database_restart() {
    use ariadne::{
        crawler::crawl,
        ingestion::extract_crawl,
        storage::{KnowledgeStore, Source},
    };
    let fixture = Fixture::start().await;
    let directory = tempfile::TempDir::new().unwrap();
    let store = KnowledgeStore::open(directory.path()).await.unwrap();
    let request = adapter_request(&fixture, "/docs/api");
    let source_id = request.source_id.clone();
    let crawl_id = request.crawl_id.clone();
    let url = request.seed.clone();
    store
        .register_source(Source::new(&source_id, "Fixture documentation", url.clone()).unwrap())
        .await
        .unwrap();
    store.begin_crawl(&request).await.unwrap();
    store
        .finish_crawl(extract_crawl(crawl(request).await.unwrap()))
        .await
        .unwrap();
    let before =
        serde_json::to_value(store.get_document(&source_id, &url).await.unwrap().unwrap()).unwrap();
    let chunks_before = store.get_chunks(&source_id, &url).await.unwrap();
    assert!(
        chunks_before
            .iter()
            .any(|chunk| chunk.text.contains("SOCKS"))
    );
    let mut search_query = ariadne::retrieval::SearchQuery::new("SOCKS");
    search_query.source_id = Some(source_id.clone());
    let search_before = ariadne::retrieval::search(&store, search_query.clone())
        .await
        .unwrap();
    assert!(!search_before.is_empty());
    drop(store);
    let mut reopened = None;
    for _ in 0..100 {
        match KnowledgeStore::open(directory.path()).await {
            Ok(store) => {
                reopened = Some(store);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let store = reopened.expect("database directory released after SDK handle drop");
    let document = store.get_document(&source_id, &url).await.unwrap().unwrap();
    assert_eq!(serde_json::to_value(&document).unwrap(), before);
    assert_eq!(
        store.get_chunks(&source_id, &url).await.unwrap(),
        chunks_before
    );
    assert_eq!(
        ariadne::retrieval::search(&store, search_query)
            .await
            .unwrap(),
        search_before
    );
    assert!(document.markdown().contains("SOCKS"));
    assert_eq!(document.sections[2].parent_id, Some(1));
    assert_eq!(
        store
            .get_crawl(&source_id, &crawl_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    assert!(
        !store
            .page_outcomes(&source_id, &crawl_id)
            .await
            .unwrap()
            .is_empty()
    );
}

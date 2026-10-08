use anyhow::Result;
use ariadne::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, CrawlScope},
    embeddings::{EmbeddingProvider, EmbeddingPurpose, EmbeddingSpace, ProviderLimits},
    ingestion::{IngestionStatus, ingest, recrawl, recrawl_lexical, reprocess_lexical},
    retrieval::{SearchQuery, search},
    storage::{KnowledgeStore, Source},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
use url::Url;

#[derive(Clone)]
struct Route {
    body: String,
    etag: Option<String>,
    modified: Option<String>,
    status: u16,
    extra: String,
    delay_ms: u64,
}
impl Route {
    fn html(body: &str) -> Self {
        Self {
            body: body.into(),
            etag: Some("\"v1\"".into()),
            modified: Some("Wed, 07 Oct 2026 08:00:00 GMT".into()),
            status: 200,
            extra: String::new(),
            delay_ms: 0,
        }
    }
}
struct Site {
    root: Url,
    routes: Arc<Mutex<HashMap<String, Route>>>,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    task: JoinHandle<()>,
}
impl Drop for Site {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Site {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = Url::parse(&format!("http://{}/docs/", listener.local_addr().unwrap())).unwrap();
        let routes = Arc::new(Mutex::new(HashMap::from([
            (
                "/docs/".into(),
                Route::html(
                    "<title>Docs</title><main><h1>Overview</h1><p>Documentation index.</p><a href='/docs/a'>Alpha</a><a href='/docs/b'>Beta</a></main>",
                ),
            ),
            (
                "/docs/a".into(),
                Route::html(
                    "<title>Alpha</title><main><h1 id='alpha'>Alpha</h1><p>AlphaOriginal explains networking.</p><pre><code>Client::alpha()</code></pre></main>",
                ),
            ),
            (
                "/docs/b".into(),
                Route::html(
                    "<title>Beta</title><main><h1>Beta</h1><p>BetaOriginal explains storage using <code>Store::open</code>.</p><a href='a'>Alpha</a></main>",
                ),
            ),
        ])));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let pages = routes.clone();
        let log = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let pages = pages.clone();
                let log = log.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    loop {
                        let mut chunk = [0; 4096];
                        let n = socket.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&chunk[..n]);
                        if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    let path = request.split_whitespace().nth(1).unwrap().to_owned();
                    log.lock().unwrap().push((path.clone(), request.clone()));
                    let route = if path == "/robots.txt" {
                        Route {
                            body: "User-agent: *\nAllow: /\n".into(),
                            etag: None,
                            modified: None,
                            status: 200,
                            extra: String::new(),
                            delay_ms: 0,
                        }
                    } else {
                        pages.lock().unwrap().get(&path).cloned().unwrap()
                    };
                    if route.delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(route.delay_ms)).await;
                    }
                    let header = |key: &str| {
                        request.lines().find_map(|line| {
                            line.split_once(':')
                                .filter(|(name, _)| name.eq_ignore_ascii_case(key))
                                .map(|(_, v)| v.trim().to_owned())
                        })
                    };
                    let matched = match &route.etag {
                        Some(etag) => header("if-none-match").as_ref() == Some(etag),
                        None => {
                            route.modified.is_some()
                                && header("if-modified-since") == route.modified
                        }
                    };
                    let status = if route.status == 200 && matched {
                        304
                    } else {
                        route.status
                    };
                    let body = if status == 304 { "" } else { &route.body };
                    let mut headers = route.extra;
                    if let Some(tag) = route.etag {
                        headers.push_str(&format!("ETag: {tag}\r\n"));
                    }
                    if let Some(date) = route.modified {
                        headers.push_str(&format!("Last-Modified: {date}\r\n"));
                    }
                    let response = format!(
                        "HTTP/1.1 {status} Response\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            root,
            routes,
            requests,
            task,
        }
    }
    fn source(&self) -> Source {
        Source::new("docs", "Fixture docs", self.root.clone()).unwrap()
    }
    fn request(&self, run: &str) -> CrawlRequest {
        let mut request = CrawlRequest::new(
            "docs",
            run,
            self.root.clone(),
            CrawlScope::new(self.root.clone(), "/docs").unwrap(),
        );
        request.max_pages = 10;
        request.allow_loopback_redirects = true;
        request
    }
    fn url(&self, path: &str) -> Url {
        self.root.join(path).unwrap()
    }
    fn change(&self, path: &str, f: impl FnOnce(&mut Route)) {
        f(self.routes.lock().unwrap().get_mut(path).unwrap());
    }
}

#[derive(Clone)]
struct Provider {
    space: EmbeddingSpace,
    calls: Arc<AtomicUsize>,
}
impl Provider {
    fn new() -> Self {
        Self {
            space: EmbeddingSpace {
                provider: "fixture".into(),
                model: "test".into(),
                revision: "v1".into(),
                dimensions: 3,
                input_version: "v1".into(),
            },
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl EmbeddingProvider for Provider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }
    fn limits(&self) -> ProviderLimits {
        ProviderLimits {
            batch_size: 2,
            max_input_bytes: 10000,
            max_batch_bytes: 20000,
        }
    }
    async fn embed(&self, input: &[String], _: EmbeddingPurpose) -> Result<Vec<Vec<f64>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(input.iter().map(|s| vec![1., s.len() as f64, 2.]).collect())
    }
}
async fn setup(site: &Site) -> (tempfile::TempDir, KnowledgeStore, Provider) {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    let provider = Provider::new();
    let p = provider.clone();
    let run = ingest(
        &store,
        site.source(),
        site.request("initial"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Completed);
    (dir, store, provider)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_recrawl_after_restart_preserves_chunks_and_generates_no_embeddings() {
    let site = Site::start().await;
    let (dir, store, provider) = setup(&site).await;
    let chunks = store.get_chunks("docs", &site.url("a")).await.unwrap();
    let links = store
        .document_links("docs", &site.root, false, 100)
        .await
        .unwrap();
    let mentions = store
        .entity_mentions("docs", "Client::alpha", 100)
        .await
        .unwrap();
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    site.requests.lock().unwrap().clear();
    let p = provider.clone();
    let calls = provider.calls.load(Ordering::SeqCst);
    let run = recrawl(
        &store,
        site.source(),
        site.request("unchanged"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    let summary = run.summary.unwrap();
    assert_eq!(summary["incremental"]["unchanged"], 3);
    assert_eq!(summary["chunk_count"], 0);
    let progress = run.ingestion.unwrap();
    assert_eq!(progress.status, IngestionStatus::Completed);
    let report = progress.embedding_report.unwrap();
    assert_eq!(report.generated, 0);
    assert_eq!(report.reused, 3);
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    assert_eq!(
        store
            .document_links("docs", &site.root, false, 100)
            .await
            .unwrap(),
        links
    );
    assert_eq!(
        store
            .entity_mentions("docs", "Client::alpha", 100)
            .await
            .unwrap(),
        mentions
    );
    assert_eq!(
        store.get_chunks("docs", &site.url("a")).await.unwrap(),
        chunks
    );
    let audits = store.page_outcomes("docs", "unchanged").await.unwrap();
    assert_eq!(audits.len(), 3);
    assert!(audits.iter().all(
        |v| v["page"]["status"] == 304 && v["page"]["raw_body"].as_array().unwrap().is_empty()
    ));
    {
        let requests = site.requests.lock().unwrap();
        for path in ["/docs/", "/docs/a", "/docs/b"] {
            let request = &requests
                .iter()
                .find(|(p, _)| p == path)
                .unwrap()
                .1
                .to_lowercase();
            assert!(request.contains("if-none-match: \"v1\""));
            assert!(request.contains("if-modified-since:"));
        }
    }
    assert_eq!(
        store
            .document_status("docs", &site.url("a"))
            .await
            .unwrap()
            .unwrap()["revalidation"]["status"],
        304
    );
    assert_eq!(
        store.source_status("docs").await.unwrap()["active_documents"],
        3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_page_replaces_only_its_chunks_and_invalidates_its_old_vectors() {
    let site = Site::start().await;
    let (_dir, store, provider) = setup(&site).await;
    let old = store.get_chunks("docs", &site.url("a")).await.unwrap();
    let unchanged = store.get_chunks("docs", &site.url("b")).await.unwrap();
    site.change("/docs/a", |r| {
        r.etag = Some("\"v2\"".into());
        r.body = r.body.replace("AlphaOriginal", "AlphaChanged");
    });
    let p = provider.clone();
    let run = recrawl(
        &store,
        site.source(),
        site.request("changed"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["incremental"]["changed"], 1);
    assert_eq!(
        run.ingestion.unwrap().embedding_report.unwrap().generated,
        1
    );
    assert_ne!(
        store.get_chunks("docs", &site.url("a")).await.unwrap()[0].id,
        old[0].id
    );
    assert_eq!(
        store.get_chunks("docs", &site.url("b")).await.unwrap(),
        unchanged
    );
    assert!(
        search(&store, SearchQuery::new("AlphaOriginal"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        search(&store, SearchQuery::new("AlphaChanged"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .embedding_coverage(&provider.space, "docs")
            .await
            .unwrap()
            .stale,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_bytes_and_boilerplate_changes_skip_derived_processing_without_validators() {
    let site = Site::start().await;
    for path in ["/docs/", "/docs/a", "/docs/b"] {
        site.change(path, |r| {
            r.etag = None;
            r.modified = None;
        });
    }
    let (_dir, store, provider) = setup(&site).await;
    let chunks = store.get_chunks("docs", &site.url("a")).await.unwrap();
    let mentions = store
        .entity_mentions("docs", "Client::alpha", 100)
        .await
        .unwrap();
    site.change("/docs/a", |r| {
        r.body = format!("<nav>Changed boilerplate</nav>{}", r.body)
    });
    let p = provider.clone();
    let run = recrawl(
        &store,
        site.source(),
        site.request("hashes"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["incremental"]["unchanged"], 3);
    assert_eq!(
        store
            .entity_mentions("docs", "Client::alpha", 100)
            .await
            .unwrap(),
        mentions
    );
    assert_eq!(
        store
            .document_links("docs", &site.url("a"), false, 20)
            .await
            .unwrap()["document"]["graph"]["version"],
        ariadne::graph::GRAPH_VERSION
    );
    assert_eq!(
        run.ingestion.unwrap().embedding_report.unwrap().generated,
        0
    );
    assert_eq!(
        store.get_chunks("docs", &site.url("a")).await.unwrap(),
        chunks
    );
    assert!(
        String::from_utf8(
            store
                .get_document("docs", &site.url("a"))
                .await
                .unwrap()
                .unwrap()
                .page
                .raw_body
        )
        .unwrap()
        .contains("Changed boilerplate")
    );
    let audits = store.page_outcomes("docs", "hashes").await.unwrap();
    assert_eq!(
        audits
            .iter()
            .filter(|v| v["reuse"] == "raw_unchanged")
            .count(),
        2
    );
    assert_eq!(
        audits
            .iter()
            .filter(|v| v["reuse"] == "normalized_unchanged")
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_modified_alone_and_304_validator_rotation_are_persisted() {
    let site = Site::start().await;
    site.change("/docs/a", |r| r.etag = None);
    let (_dir, store, _provider) = setup(&site).await;
    site.change("/docs/b", |r| {
        r.status = 304;
        r.etag = Some("W/\"v2\"".into());
    });
    let run = recrawl_lexical(
        &store,
        site.source(),
        site.request("rotation"),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["incremental"]["unchanged"], 3);
    site.change("/docs/b", |r| r.status = 200);
    site.requests.lock().unwrap().clear();
    recrawl_lexical(
        &store,
        site.source(),
        site.request("next"),
        Default::default(),
    )
    .await
    .unwrap();
    let requests = site.requests.lock().unwrap();
    let request = &requests
        .iter()
        .find(|(p, _)| p == "/docs/b")
        .unwrap()
        .1
        .to_lowercase();
    assert!(request.contains("if-none-match: w/\"v2\""));
    let request = &requests
        .iter()
        .find(|(p, _)| p == "/docs/a")
        .unwrap()
        .1
        .to_lowercase();
    assert!(!request.contains("if-none-match"));
    assert!(request.contains("if-modified-since"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gone_pages_leave_search_but_keep_raw_content_and_can_be_restored() {
    let site = Site::start().await;
    let (_dir, store, provider) = setup(&site).await;
    assert_eq!(
        store
            .entity_mentions("docs", "Store::open", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    site.change("/docs/b", |r| r.status = 410);
    site.change("/docs/a", |r| r.status = 503);
    let run = recrawl_lexical(
        &store,
        site.source(),
        site.request("gone"),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Partial);
    let summary = run.summary.unwrap();
    assert_eq!(summary["removed_count"], 1);
    assert_eq!(summary["rejected_count"], 1);
    assert!(
        store
            .entity_mentions("docs", "Store::open", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .entity_mentions("docs", "Client::alpha", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let links = store
        .document_links("docs", &site.root, false, 20)
        .await
        .unwrap();
    assert_eq!(
        links["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["target_url"] == site.url("b").as_str())
            .unwrap()["target_availability"],
        "removed"
    );
    assert!(
        store
            .document_links("docs", &site.url("b"), true, 20)
            .await
            .unwrap()["links"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let incoming = store
        .document_links("docs", &site.url("a"), true, 20)
        .await
        .unwrap();
    assert_eq!(incoming["links"].as_array().unwrap().len(), 1);
    assert_eq!(incoming["links"][0]["document_url"], site.root.as_str());
    assert!(
        search(&store, SearchQuery::new("BetaOriginal"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        search(&store, SearchQuery::new("AlphaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .get_document("docs", &site.url("b"))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .document_status("docs", &site.url("b"))
            .await
            .unwrap()
            .unwrap()["revalidation"]["availability"],
        "removed"
    );
    assert_eq!(
        store
            .embedding_coverage(&provider.space, "docs")
            .await
            .unwrap()
            .stale,
        1
    );
    assert_eq!(
        store.source_status("docs").await.unwrap()["removed_documents"],
        1
    );
    site.change("/docs/b", |r| r.status = 200);
    site.change("/docs/a", |r| r.status = 200);
    let p = provider.clone();
    let run = recrawl(
        &store,
        site.source(),
        site.request("restored"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .entity_mentions("docs", "Store::open", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        run.ingestion.unwrap().embedding_report.unwrap().generated,
        1
    );
    assert_eq!(
        search(&store, SearchQuery::new("BetaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store.source_status("docs").await.unwrap()["removed_documents"],
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budgeted_discovery_never_removes_unvisited_documents() {
    let site = Site::start().await;
    let (_dir, store, _provider) = setup(&site).await;
    site.change("/docs/", |r| {
        r.etag = Some("\"v2\"".into());
        r.body = "<main><h1>Overview</h1><p>No more links.</p></main>".into();
    });
    let mut request = site.request("budget");
    request.max_pages = 1;
    let run = recrawl_lexical(&store, site.source(), request, Default::default())
        .await
        .unwrap();
    let summary = run.summary.unwrap();
    assert_eq!(summary["budget_reached"], true);
    assert_eq!(summary["removed_count"], 0);
    assert_eq!(
        search(&store, SearchQuery::new("BetaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store.source_status("docs").await.unwrap()["active_documents"],
        3
    );
    // A subsequent bounded refresh explicitly revisits known pages even when
    // current navigation no longer links to them.
    site.change("/docs/b", |r| r.status = 404);
    let run = recrawl_lexical(
        &store,
        site.source(),
        site.request("known"),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["removed_count"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_changes_reprocess_304_content_and_offline_reprocessing_needs_no_server() {
    let site = Site::start().await;
    let (_dir, store, _provider) = setup(&site).await;
    let old = store.get_chunks("docs", &site.url("a")).await.unwrap();
    let run = recrawl_lexical(
        &store,
        site.source(),
        site.request("policy"),
        ChunkPolicy { target_chars: 40 },
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["incremental"]["reprocessed"], 3);
    let audits = store.page_outcomes("docs", "policy").await.unwrap();
    assert!(
        audits
            .iter()
            .all(|v| v["page"]["status"] == 304 && v["processing"] == "retained_raw")
    );
    assert_ne!(
        store.get_chunks("docs", &site.url("a")).await.unwrap()[0].id,
        old[0].id
    );
    site.requests.lock().unwrap().clear();
    let checked = store
        .document_status("docs", &site.url("a"))
        .await
        .unwrap()
        .unwrap()["revalidation"]
        .clone();
    site.task.abort();
    let run = reprocess_lexical(
        &store,
        site.source(),
        site.request("offline"),
        ChunkPolicy { target_chars: 80 },
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["incremental"]["reprocessed"], 3);
    assert!(site.requests.lock().unwrap().is_empty());
    assert_eq!(
        store
            .document_status("docs", &site.url("a"))
            .await
            .unwrap()
            .unwrap()["revalidation"],
        checked
    );
    assert_eq!(
        store
            .get_indexing("docs", &site.url("a"))
            .await
            .unwrap()
            .unwrap()
            .policy
            .target_chars,
        80
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_store_and_vary_pages_are_fetched_unconditionally_and_invalid_304_is_rejected() {
    let site = Site::start().await;
    site.change("/docs/a", |r| {
        r.extra = "Cache-Control: no-store\r\n".into()
    });
    site.change("/docs/b", |r| r.extra = "Vary: User-Agent\r\n".into());
    let (_dir, store, _provider) = setup(&site).await;
    site.requests.lock().unwrap().clear();
    site.change("/docs/b", |r| r.status = 304);
    let run = recrawl_lexical(
        &store,
        site.source(),
        site.request("cache-policy"),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["rejected_count"], 1);
    assert_eq!(
        search(&store, SearchQuery::new("BetaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
    let requests = site.requests.lock().unwrap();
    for path in ["/docs/a", "/docs/b"] {
        assert!(
            !requests
                .iter()
                .find(|(p, _)| p == path)
                .unwrap()
                .1
                .to_lowercase()
                .contains("if-none-match")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_recrawl_preserves_knowledge_and_prevents_a_second_active_source_run() {
    let site = Site::start().await;
    let (dir, store, _provider) = setup(&site).await;
    let store = Arc::new(store);
    let worker_store = store.clone();
    for path in ["/docs/", "/docs/a", "/docs/b"] {
        site.change(path, |r| r.delay_ms = 1000);
    }
    site.requests.lock().unwrap().clear();
    let source = site.source();
    let request = site.request("interrupted");
    let task = tokio::spawn(async move {
        recrawl_lexical(&worker_store, source, request, Default::default()).await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if site
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|(path, _)| path.starts_with("/docs"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(store.begin_crawl(&site.request("overlap")).await.is_err());
    assert!(store.get_crawl("docs", "overlap").await.unwrap().is_none());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(store);
    let mut reopened = None;
    for _ in 0..50 {
        if let Ok(store) = KnowledgeStore::open(dir.path().join("knowledge")).await {
            reopened = Some(store);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let store = reopened.expect("database released after cancellation");
    let run = store
        .get_crawl("docs", "interrupted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "interrupted");
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Interrupted);
    assert_eq!(
        search(&store, SearchQuery::new("AlphaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        search(&store, SearchQuery::new("BetaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revalidation_keeps_redirect_scope_and_body_limits_and_audit_overflow_is_explicit() {
    let site = Site::start().await;
    let (_dir, store, _provider) = setup(&site).await;
    site.change("/docs/a", |r| {
        r.status = 302;
        r.extra = "Location: /outside\r\n".into();
    });
    let run = recrawl_lexical(
        &store,
        site.source(),
        site.request("redirect"),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Partial);
    assert!(
        run.summary.unwrap()["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["reason"] == "RedirectScope")
    );
    assert!(
        !site
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(p, _)| p == "/outside")
    );
    assert_eq!(
        search(&store, SearchQuery::new("AlphaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
    site.change("/docs/a", |r| {
        r.status = 200;
        r.extra.clear();
        r.etag = Some("\"v2\"".into());
        r.body = "x".repeat(2000);
    });
    let mut request = site.request("oversized");
    request.max_retained_body_bytes = 1000;
    let run = recrawl_lexical(&store, site.source(), request, Default::default())
        .await
        .unwrap();
    assert_eq!(run.summary.unwrap()["rejected_count"], 1);
    assert_eq!(
        search(&store, SearchQuery::new("AlphaOriginal"))
            .await
            .unwrap()
            .len(),
        1
    );
    let mut request = site.request("overflow");
    request.audit_capacity = 1;
    let run = recrawl_lexical(&store, site.source(), request, Default::default())
        .await
        .unwrap();
    assert_eq!(run.summary.unwrap()["audit_overflow"], true);
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Partial);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_revision_changes_reembed_unchanged_pages_in_a_separate_space() {
    let site = Site::start().await;
    let (_dir, store, provider) = setup(&site).await;
    let mut next = provider.clone();
    next.space.revision = "v2".into();
    let p = next.clone();
    let run = recrawl(
        &store,
        site.source(),
        site.request("new-model"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["incremental"]["unchanged"], 3);
    assert_eq!(
        run.ingestion.unwrap().embedding_report.unwrap().generated,
        3
    );
    assert_eq!(
        store
            .embedding_coverage(&provider.space, "docs")
            .await
            .unwrap()
            .ready,
        3
    );
    assert_eq!(
        store
            .embedding_coverage(&next.space, "docs")
            .await
            .unwrap()
            .ready,
        3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_recrawl_and_status_reopen_storage_and_offline_reprocess_works() {
    let site = Site::start().await;
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_Ariadne"));
        command.args(args).env("ARIADNE_DATA_DIR", dir.path());
        command
    };
    for args in [
        vec!["source", "add", "docs", "Fixture docs", site.root.as_str()],
        vec!["ingest", "docs", "initial", "--lexical-only"],
        vec!["recrawl", "docs", "refresh", "--lexical-only"],
    ] {
        let output = run(&args).output().await.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let status: serde_json::Value = serde_json::from_slice(
        &run(&["source", "status", "docs"])
            .output()
            .await
            .unwrap()
            .stdout,
    )
    .unwrap();
    assert_eq!(status["active_documents"], 3);
    assert_eq!(status["latest_run"]["crawl_id"], "refresh");
    let output = run(&["document-status", "docs", site.url("a").as_str()])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["revalidation"]["status"], 304);
    site.task.abort();
    let output = run(&["reprocess", "docs", "offline", "--chunk-chars", "80"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let run: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(run["ingestion"]["operation"], "reprocess");
    assert_eq!(run["summary"]["incremental"]["reprocessed"], 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_extraction_version_is_reprocessed_even_when_http_returns_304() {
    use ariadne::{extraction::ExtractionOutcome, ingestion::ExtractionBatch};
    use std::time::SystemTime;
    let site = Site::start().await;
    let (_dir, store, provider) = setup(&site).await;
    let mut document = store
        .get_document("docs", &site.url("a"))
        .await
        .unwrap()
        .unwrap();
    document.extraction_version = "previous-extractor".into();
    document.page.crawl_id = "legacy".into();
    let request = site.request("legacy");
    store.begin_crawl(&request).await.unwrap();
    let now = SystemTime::now();
    store
        .finish_crawl(ExtractionBatch {
            source_id: "docs".into(),
            crawl_id: "legacy".into(),
            started_at: now,
            finished_at: now,
            outcomes: vec![ExtractionOutcome::Extracted(Box::new(document))],
            blocked: vec![],
            dropped_pages: 0,
            audit_overflow: false,
            discovery: Default::default(),
        })
        .await
        .unwrap();
    let p = provider.clone();
    let run = recrawl(
        &store,
        site.source(),
        site.request("upgrade"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    let summary = run.summary.unwrap();
    assert_eq!(summary["incremental"]["reprocessed"], 1);
    assert_eq!(summary["incremental"]["unchanged"], 2);
    assert_eq!(
        store
            .get_indexing("docs", &site.url("a"))
            .await
            .unwrap()
            .unwrap()
            .extraction_version,
        ariadne::extraction::EXTRACTION_VERSION
    );
    assert!(
        store
            .page_outcomes("docs", "upgrade")
            .await
            .unwrap()
            .iter()
            .all(|v| v["page"]["status"] == 304)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_validators_are_ignored_and_hash_comparison_still_reuses_content() {
    let site = Site::start().await;
    site.change("/docs/a", |route| {
        route.etag = Some("*".into());
        route.modified = Some("invalid-date".into());
    });
    let (_dir, store, provider) = setup(&site).await;
    site.requests.lock().unwrap().clear();
    let p = provider.clone();
    let run = recrawl(
        &store,
        site.source(),
        site.request("invalid-validators"),
        Default::default(),
        || async { Ok(p) },
    )
    .await
    .unwrap();
    assert_eq!(run.summary.unwrap()["incremental"]["unchanged"], 3);
    assert_eq!(
        run.ingestion.unwrap().embedding_report.unwrap().generated,
        0
    );
    let requests = site.requests.lock().unwrap();
    let (_, request) = requests.iter().find(|(path, _)| path == "/docs/a").unwrap();
    assert!(!request.to_ascii_lowercase().contains("if-none-match:"));
    assert!(!request.to_ascii_lowercase().contains("if-modified-since:"));
}

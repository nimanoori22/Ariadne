//! Opt-in real Chromium acceptance, served entirely by a deterministic local site.
use ariadne::{
    crawler::{CrawlRequest, CrawlScope},
    ingestion::{IngestionStatus, ingest_lexical, recrawl_lexical},
    retrieval::{SearchQuery, search},
    storage::{KnowledgeStore, Source},
};
use std::{
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
struct Fixture {
    root: Url,
    requests: Arc<Mutex<Vec<(String, bool)>>>,
    version: Arc<AtomicUsize>,
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
        let root = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(vec![]));
        let version = Arc::new(AtomicUsize::new(1));
        let (log, revision) = (requests.clone(), version.clone());
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (log, revision) = (log.clone(), revision.clone());
                tokio::spawn(async move {
                    let mut raw = vec![];
                    loop {
                        let mut buffer = [0; 4096];
                        let n = socket.read(&mut buffer).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        raw.extend_from_slice(&buffer[..n]);
                        if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let headers = String::from_utf8_lossy(&raw);
                    let path = headers.split_whitespace().nth(1).unwrap().to_owned();
                    let conditional = headers.to_lowercase().contains("if-none-match:");
                    log.lock().unwrap().push((path.clone(), conditional));
                    let(status,kind,body)=match path.as_str(){
     "/robots.txt"=>(200,"text/plain","User-agent: *\nDisallow: /docs/blocked\n".into()),
     "/docs/" if conditional=>(304,"text/html",String::new()),
     "/docs/"=>(200,"text/html","<html><title>Dynamic documentation</title><main id='app'></main><script src='/docs/app.js'></script></html>".into()),
     "/docs/app.js"=>(200,"text/javascript","fetch('/outside').catch(()=>{});fetch('/docs/blocked').catch(()=>{});fetch('/docs/redirect-blocked').catch(()=>{});new WebSocket('ws://'+location.host+'/docs/socket');fetch('/docs/data').then(r=>r.text()).then(t=>{document.getElementById('app').innerHTML='<h1 id=\"client\">Dynamic client</h1><p>'+t+'</p><pre><code>Client::new()</code></pre><a href=\"/docs/api\">API</a>';});".into()),
     "/docs/redirect-blocked"=>(302,"text/plain",String::new()),
     "/docs/data"=>(200,"text/plain",format!("Configure DynamicClient with version{} and preserve generated examples for retrieval.",revision.load(Ordering::SeqCst))),
     "/docs/api"=>(200,"text/html","<html><title>Static API</title><main><h1>API Reference</h1><p>This is a useful static API document that does not need browser rendering.</p></main></html>".into()),
     "/docs/stuck"=>(200,"text/html","<html><main id='app'>Loading documentation...</main><script>setInterval(()=>{},1000)</script></html>".into()),
     _=>(404,"text/plain","missing".into())};
                    let redirect = if path == "/docs/redirect-blocked" {
                        "Location: /docs/blocked\r\n"
                    } else {
                        ""
                    };
                    let reply = format!(
                        "HTTP/1.1 {status} Response\r\n{redirect}Content-Type: {kind}\r\nETag: \"shell-v1\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                });
            }
        });
        Self {
            root,
            requests,
            version,
            task,
        }
    }
    fn source(&self) -> Source {
        Source::new("docs", "Dynamic fixture", self.root.join("docs/").unwrap()).unwrap()
    }
    fn request(&self, id: &str) -> CrawlRequest {
        let source = self.source();
        let mut request = CrawlRequest::new(
            "docs",
            id,
            source.root_url.clone(),
            CrawlScope::new(source.root_url, "/docs").unwrap(),
        );
        request.max_pages = 3;
        request.browser_fallback = true;
        request.allow_loopback_redirects = true;
        request
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Chromium with its sandbox available"]
async fn real_browser_renders_discovers_preserves_scope_and_recrawls_dynamic_content() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    let run = ingest_lexical(&store, f.source(), f.request("first"), Default::default())
        .await
        .unwrap();
    assert_eq!(
        run.ingestion.unwrap().status,
        IngestionStatus::Completed,
        "{:?}",
        run.summary
    );
    assert_eq!(run.summary.unwrap()["document_count"], 2);
    let raw = store
        .get_document("docs", &f.source().root_url)
        .await
        .unwrap()
        .unwrap();
    let raw = serde_json::to_value(raw).unwrap();
    assert_eq!(raw["page"]["rendering"]["outcome"], "rendered");
    assert!(
        !raw["page"]["rendering"]["original_body"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let hits = search(&store, SearchQuery::new("DynamicClient"))
        .await
        .unwrap();
    assert!(!hits.is_empty());
    assert!(hits[0].text.contains("version1"));
    assert!(
        hits[0]
            .url
            .as_str()
            .starts_with(f.source().root_url.as_str())
    );
    f.version.store(2, Ordering::SeqCst);
    let run = recrawl_lexical(&store, f.source(), f.request("second"), Default::default())
        .await
        .unwrap();
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Completed);
    let hits = search(&store, SearchQuery::new("DynamicClient"))
        .await
        .unwrap();
    assert!(hits[0].text.contains("version2"));
    assert!(!hits[0].text.contains("version1"));
    let requests = f.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .filter(|(p, _)| p == "/docs/")
            .all(|(_, conditional)| !*conditional)
    );
    assert!(
        !requests
            .iter()
            .any(|(p, _)| ["/outside", "/docs/blocked", "/docs/socket"].contains(&p.as_str()))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Chromium with its sandbox available"]
async fn render_deadline_audits_failure_without_indexing_a_loading_shell() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    let source = Source::new("stuck", "Stuck", f.root.join("docs/stuck").unwrap()).unwrap();
    let mut request = CrawlRequest::new(
        "stuck",
        "timeout",
        source.root_url.clone(),
        CrawlScope::new(source.root_url.clone(), "/docs/stuck").unwrap(),
    );
    request.browser_fallback = true;
    request.max_pages = 1;
    request.request_timeout = Duration::from_secs(3);
    let run = ingest_lexical(&store, source, request, Default::default())
        .await
        .unwrap();
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Partial);
    let audit = store.page_outcomes("stuck", "timeout").await.unwrap();
    assert_eq!(audit[0]["page"]["rendering"]["outcome"], "failed");
    assert_eq!(audit[0]["page"]["state"], "RenderFailure");
    assert!(
        search(&store, SearchQuery::new("Loading"))
            .await
            .unwrap()
            .is_empty()
    );
}

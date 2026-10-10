use ariadne::{
    crawler::{CrawlRequest, CrawlScope},
    embeddings::OllamaConfig,
    ingestion::crawl_site,
    jobs::CrawlAccess,
    mcp::KnowledgeMcp,
    retrieval::{SearchQuery, search},
    storage::{KnowledgeStore, Source},
};
use rmcp::{ServiceExt, model::CallToolRequestParams};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use url::Url;
struct Site {
    root: Url,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Site {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Site {
    async fn start(pages: usize, slow: bool, fail: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = Url::parse(&format!("http://{}/en/", listener.local_addr().unwrap())).unwrap();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = [0; 8192];
                    let n = socket.read(&mut buf).await.unwrap();
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req.split_whitespace().nth(1).unwrap();
                    let (status, body) = if path == "/robots.txt" {
                        (200, "User-agent: *\nDisallow: /en/private\n".into())
                    } else if path == "/en/" {
                        let links = (0..pages)
                            .map(|n| format!("<a href='page/{n}'>Page {n}</a>"))
                            .collect::<String>();
                        (
                            200,
                            format!(
                                "<nav>{links}<a href='/ja/'>Japanese</a><a href='/en-other/'>Outside</a><a href='/en/private'>Private</a><a href='/en/report.pdf'>Report</a><a href='/en/table.xlsx'>Table</a></nav><main><h1>English Index</h1><p>English documentation index.</p></main>"
                            ),
                        )
                    } else if let Some(n) = path
                        .strip_prefix("/en/page/")
                        .and_then(|n| n.parse::<usize>().ok())
                    {
                        if slow && n >= 20 {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        if fail && n == 3 {
                            (503, "unavailable".into())
                        } else {
                            (
                                200,
                                format!(
                                    "<main><h1>Page {n}</h1><p>UniqueMarker{n} English body.</p><a href='/en/'>Home</a></main>"
                                ),
                            )
                        }
                    } else {
                        panic!("out of scope URL fetched: {path}")
                    };
                    let response = format!(
                        "HTTP/1.1 {status} Response\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self { root, task }
    }
    fn source(&self) -> Source {
        Source::new("english", "English", self.root.clone()).unwrap()
    }
    fn request(&self, run: &str) -> CrawlRequest {
        let mut request = CrawlRequest::new(
            "english",
            run,
            self.root.clone(),
            CrawlScope::new(self.root.clone(), "/en/").unwrap(),
        );
        request.allow_loopback_redirects = true;
        request
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_html_crawl_exceeds_100_and_persists_navigation_discovery_failures_and_scope() {
    let site = Site::start(125, false, true).await;
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path()).await.unwrap();
    let run = crawl_site(
        &store,
        site.source(),
        site.request("all"),
        Default::default(),
        false,
    )
    .await
    .unwrap();
    let coverage = store.site_coverage("english", "all").await.unwrap();
    assert_eq!(coverage["pending"], 0);
    assert_eq!(coverage["done"], 125);
    assert_eq!(coverage["failed"], 1);
    assert_eq!(coverage["blocked"], 1);
    let summary = run.summary.unwrap();
    assert_eq!(summary["page_count"], 127);
    assert_eq!(summary["frontier_exhausted"], true);
    assert_eq!(summary["budget_reached"], false);
    assert_eq!(summary["document_count"], 125);
    assert_eq!(
        summary["frontier"],
        json!({"pending":0,"done":125,"failed":1,"blocked":1,"skipped":0})
    );
    assert_eq!(
        store.page_outcomes("english", "all").await.unwrap().len(),
        127
    );
    let hits = search(&store, SearchQuery::new("UniqueMarker124"))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(
        hits.iter()
            .all(|h| h.document_url.path().starts_with("/en/"))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_site_crawl_resumes_committed_frontier_after_reopen() {
    let site = Site::start(45, true, false).await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KnowledgeStore::open(dir.path()).await.unwrap());
    let worker_store = store.clone();
    let source = site.source();
    let request = site.request("resume");
    let task = tokio::spawn(async move {
        crawl_site(&worker_store, source, request, Default::default(), false).await
    });
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(run) = store.get_crawl("english", "resume").await.unwrap()
                && run
                    .summary
                    .as_ref()
                    .is_some_and(|s| s["page_count"].as_u64().unwrap_or(0) > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    let before = store.site_coverage("english", "resume").await.unwrap();
    assert!(before["pending"].as_u64().unwrap() > 0);
    let count = store
        .page_outcomes("english", "resume")
        .await
        .unwrap()
        .len();
    drop(store);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let store = KnowledgeStore::open(dir.path()).await.unwrap();
    assert_eq!(
        store
            .get_crawl("english", "resume")
            .await
            .unwrap()
            .unwrap()
            .status,
        "interrupted"
    );
    let run = crawl_site(
        &store,
        site.source(),
        site.request("resume"),
        Default::default(),
        true,
    )
    .await
    .unwrap();
    assert!(run.summary.unwrap()["page_count"].as_u64().unwrap() > count as u64);
    let outcomes = store.page_outcomes("english", "resume").await.unwrap();
    assert_eq!(outcomes.len(), 47);
    let unique = outcomes
        .iter()
        .map(|p| p["page"]["requested_url"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), outcomes.len());
    assert_eq!(
        store.site_coverage("english", "resume").await.unwrap()["pending"],
        0
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_full_site_job_keeps_search_available_and_reports_partial_coverage() {
    let site = Site::start(105, false, true).await;
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path()).await.unwrap();
    store.register_source(site.source()).await.unwrap();
    let access = CrawlAccess {
        allowed_sources: ["english".into()].into(),
        allow_private_network: true,
        browser_fallback: false,
    };
    let (a, b) = tokio::io::duplex(65536);
    let server = tokio::spawn(
        KnowledgeMcp::with_access(store, OllamaConfig::from_env().unwrap(), access).serve(a),
    );
    let client = ().serve(b).await.unwrap();
    let server = server.await.unwrap().unwrap();
    let call = |name: &str, args: serde_json::Value| {
        CallToolRequestParams::new(name.to_owned())
            .with_arguments(args.as_object().unwrap().clone())
    };
    let started = client
        .call_tool(call("crawl_site", json!({"source_id":"english"})))
        .await
        .unwrap();
    assert_ne!(started.is_error, Some(true), "{started:?}");
    let id = started.structured_content.unwrap()["job"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = client
                .call_tool(call("job_status", json!({"job_id":id})))
                .await
                .unwrap()
                .structured_content
                .unwrap();
            if !["queued", "running"].contains(&status["job"]["status"].as_str().unwrap()) {
                break status;
            }
            let search = client
                .call_tool(call("search", json!({"query":"English"})))
                .await
                .unwrap();
            assert_ne!(search.is_error, Some(true));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(result["job"]["status"], "partial");
    assert_eq!(result["run"]["summary"]["frontier_exhausted"], true);
    assert!(result["run"]["summary"]["page_count"].as_u64().unwrap() > 100);
    let hits = client
        .call_tool(call("search", json!({"query":"UniqueMarker104"})))
        .await
        .unwrap();
    assert_ne!(hits.is_error, Some(true));
    assert!(
        !hits.structured_content.unwrap()["hits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    client.cancel().await.unwrap();
    server.cancel().await.unwrap();
}

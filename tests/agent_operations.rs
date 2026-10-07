//! Durable agent operations against bounded local HTTP fixtures and real MCP.
use ariadne::{
    embeddings::OllamaConfig,
    jobs::{CrawlAccess, JobManager, JobOperation, JobStatus},
    mcp::KnowledgeMcp,
    storage::{KnowledgeStore, Source},
};
use rmcp::{ServiceExt, model::CallToolRequestParams};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
};
use url::Url;
struct Fixture {
    root: Url,
    task: JoinHandle<()>,
    requests: Arc<Mutex<Vec<String>>>,
    hold: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        self.release.notify_waiters();
    }
}
impl Fixture {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(vec![]));
        let hold = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (log, blocked, signal, gate) = (
            requests.clone(),
            hold.clone(),
            entered.clone(),
            release.clone(),
        );
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (log, blocked, signal, gate) =
                    (log.clone(), blocked.clone(), signal.clone(), gate.clone());
                tokio::spawn(async move {
                    let mut bytes = vec![];
                    let (end, len) = loop {
                        let mut buf = [0; 4096];
                        let n = socket.read(&mut buf).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&buf[..n]);
                        if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&bytes[..end]);
                            let len = headers
                                .lines()
                                .find_map(|s| {
                                    s.to_lowercase()
                                        .strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if bytes.len() >= end + 4 + len {
                                break (end + 4, len);
                            }
                        }
                    };
                    let path = String::from_utf8_lossy(&bytes)
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .to_owned();
                    log.lock().unwrap().push(path.clone());
                    if blocked.load(Ordering::SeqCst) && path.ends_with('/') {
                        signal.notify_one();
                        gate.notified().await;
                    }
                    let(status,kind,body)=match path.as_str(){
    "/robots.txt"=>(200,"text/plain","User-agent: *\nDisallow: /alpha/blocked\nDisallow: /beta/blocked\nAllow: /\n".into()),
    "/alpha/"|"/beta/"=>(200,"text/html",format!("<html><title>{path}</title><main><h1 id='overview'>Overview</h1><p>Configure Widget::new for your application.</p></main></html>")),
    "/alpha/api"|"/beta/api"=>(200,"text/html","<html><title>Widget API</title><main><h1 id='widget'>Widget API</h1><p>Use Widget::new to initialize a reusable client.</p><pre><code>let client = Widget::new();</code></pre></main></html>".into()),
    "/alpha/llms.txt"|"/beta/llms.txt"=>(200,"text/plain","# Documentation\n[API](api#widget)\n[Outside](/outside)\n[Sibling](/other/nope)\n[Blocked](blocked)\n".into()),
    "/alpha/llms-full.txt"|"/beta/llms-full.txt"=>(200,"text/plain","x".repeat(512*1024+1)),
    "/alpha/sitemap.xml"|"/beta/sitemap.xml"=>(200,"application/xml","<sitemapindex><sitemap><loc>child.xml</loc></sitemap></sitemapindex>".into()),
    "/alpha/child.xml"|"/beta/child.xml"=>(200,"application/xml","<urlset><url><loc>api</loc></url></urlset>".into()),
    "/api/tags"=>(200,"application/json",json!({"models":[{"name":"fixture:latest","digest":"a".repeat(64)}]}).to_string()),
    "/api/embed"=>{let body:Value=serde_json::from_slice(&bytes[end..end+len]).unwrap();let vectors:Vec<_>=body["input"].as_array().unwrap().iter().map(|_|vec![1.,0.,0.]).collect();(200,"application/json",json!({"model":"fixture:latest","embeddings":vectors}).to_string())},
    _=>(404,"text/plain","missing".into())};
                    let reply = format!(
                        "HTTP/1.1 {status} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                });
            }
        });
        Self {
            root,
            task,
            requests,
            hold,
            entered,
            release,
        }
    }
    fn config(&self) -> OllamaConfig {
        OllamaConfig {
            endpoint: self.root.clone(),
            model: "fixture".into(),
        }
    }
    async fn sources(&self, store: &KnowledgeStore) {
        for id in ["alpha", "beta"] {
            store
                .register_source(
                    Source::new(id, id, self.root.join(&format!("{id}/")).unwrap()).unwrap(),
                )
                .await
                .unwrap();
        }
    }
    fn access(&self) -> CrawlAccess {
        CrawlAccess {
            allowed_sources: ["alpha".into(), "beta".into()].into(),
            allow_private_network: true,
            browser_fallback: false,
        }
    }
}
fn arguments(name: &str, args: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_owned()).with_arguments(args.as_object().unwrap().clone())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_scoped_sources_can_be_crawled_recrawled_and_read_over_mcp() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    f.sources(&store).await;
    let (a, b) = tokio::io::duplex(65536);
    let server = tokio::spawn(KnowledgeMcp::with_access(store, f.config(), f.access()).serve(a));
    let client = ().serve(b).await.unwrap();
    let server = server.await.unwrap().unwrap();
    assert_eq!(client.list_tools(None).await.unwrap().tools.len(), 9);
    let mut ids = vec![];
    for source in ["alpha", "beta"] {
        let started = Instant::now();
        let result = client
            .call_tool(arguments(
                "crawl",
                json!({"source_id":source,"max_pages":3,"discover":true}),
            ))
            .await
            .unwrap();
        assert_ne!(result.is_error, Some(true), "{result:?}");
        println!(
            "{source} crawl admission: {} ms",
            started.elapsed().as_millis()
        );
        let id = result.structured_content.unwrap()["job"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        ids.push(id.clone());
        let status = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let result = client
                    .call_tool(arguments("job_status", json!({"job_id":id})))
                    .await
                    .unwrap();
                assert_ne!(result.is_error, Some(true), "{result:?}");
                let v = result.structured_content.unwrap();
                if !["queued", "running"].contains(&v["job"]["status"].as_str().unwrap()) {
                    break v;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            status["job"]["status"],
            "completed",
            "{status}; requests={:?}",
            f.requests.lock().unwrap()
        );
        assert_eq!(
            status["run"]["summary"]["document_count"],
            2,
            "{status}; requests={:?}",
            f.requests.lock().unwrap()
        );
        assert!(
            status["run"]["summary"]["retained_body_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(status["run"]["summary"]["crawl_elapsed_ms"].is_number());
        assert!(status["job"]["elapsed_ms"].is_number());
        assert!(
            status["run"]["summary"]["discovery"]["attempts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|a| a["outcome"] == "too_large")
        );
        let hit=client.call_tool(arguments("search",json!({"query":"Widget::new","mode":"hybrid","source_id":source,"filter":{"url_prefix":f.root.join(&format!("{source}/api")).unwrap()},"context":{"max_total_chars":1000}}))).await.unwrap().structured_content.unwrap();
        assert_eq!(hit["hits"][0]["source_id"], source);
        assert_eq!(hit["context"]["passages"][0]["source_id"], source);
        let url = f.root.join(&format!("{source}/api")).unwrap();
        let doc = client
            .call_tool(arguments(
                "get_document",
                json!({"source_id":source,"url":url,"max_text_chars":30}),
            ))
            .await
            .unwrap()
            .structured_content
            .unwrap();
        assert!(doc["text_chars"].as_u64().unwrap() <= 30);
        assert_eq!(doc["truncated"], true, "{doc}");
        assert!(doc.get("raw_content").is_none());
        let section = doc["sections"][0]["id"].clone();
        let section = client
            .call_tool(arguments(
                "get_section",
                json!({"source_id":source,"url":url,"section_id":section}),
            ))
            .await
            .unwrap()
            .structured_content
            .unwrap();
        assert_eq!(section["content_kind"], "source_data");
        let job = client
            .call_tool(arguments(
                "recrawl",
                json!({"source_id":source,"max_pages":3}),
            ))
            .await
            .unwrap()
            .structured_content
            .unwrap();
        let job = job["job"]["id"].as_str().unwrap();
        let status = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let v = client
                    .call_tool(arguments("job_status", json!({"job_id":job})))
                    .await
                    .unwrap()
                    .structured_content
                    .unwrap();
                if v["job"]["status"] != "running" && v["job"]["status"] != "queued" {
                    break v;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            status["job"]["status"],
            "completed",
            "{status}; requests={:?}",
            f.requests.lock().unwrap()
        );
        assert_eq!(
            status["run"]["ingestion"]["embedding_report"]["generated"],
            0
        );
        let status = client
            .call_tool(arguments("source_status", json!({"source_id":source})))
            .await
            .unwrap()
            .structured_content
            .unwrap();
        assert_eq!(status["active_documents"], 2);
        assert_eq!(status["recent_jobs"].as_array().unwrap().len(), 2);
    }
    let sources = client
        .call_tool(arguments("list_sources", json!({"limit":1})))
        .await
        .unwrap()
        .structured_content
        .unwrap();
    assert_eq!(sources["sources"][0]["id"], "alpha");
    let next = client
        .call_tool(arguments(
            "list_sources",
            json!({"limit":1,"after":sources["next_cursor"]}),
        ))
        .await
        .unwrap()
        .structured_content
        .unwrap();
    assert_eq!(next["sources"][0]["id"], "beta");
    assert_eq!(next["next_cursor"], Value::Null);
    for (tool, args) in [
        ("crawl", json!({"source_id":"alpha","max_pages":201})),
        ("crawl", json!({"source_id":"alpha","url":"http://evil/"})),
        ("get_section", json!({"source_id":"alpha","url":f.root})),
        (
            "get_document",
            json!({"source_id":"alpha","url":"file:///etc/passwd"}),
        ),
        ("list_sources", json!({"limit":101})),
        ("job_status", json!({"job_id":"x; SELECT *"})),
    ] {
        assert!(
            client.call_tool(arguments(tool, args)).await.is_err(),
            "{tool}"
        );
    }
    {
        let log = f.requests.lock().unwrap();
        assert!(
            !log.iter()
                .any(|p| p == "/outside" || p == "/alpha/blocked" || p == "/other/nope")
        );
    }
    client.cancel().await.unwrap();
    server.cancel().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    for id in ids {
        assert_eq!(
            store.get_job(&id).await.unwrap().unwrap().job.status,
            JobStatus::Completed
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_is_prompt_cancellation_is_durable_and_limits_are_enforced() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KnowledgeStore::open(dir.path().join("db")).await.unwrap());
    f.sources(&store).await;
    let denied = JobManager::new(store.clone(), f.config(), Default::default());
    assert!(
        denied
            .start("alpha", JobOperation::Crawl, 2, true, false)
            .await
            .is_err()
    );
    let public = JobManager::new(
        store.clone(),
        f.config(),
        CrawlAccess {
            allowed_sources: ["alpha".into()].into(),
            allow_private_network: false,
            browser_fallback: false,
        },
    );
    assert!(
        public
            .start("alpha", JobOperation::Crawl, 2, true, false)
            .await
            .is_err()
    );
    assert!(
        store.source_status("alpha").await.unwrap()["recent_jobs"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let jobs = JobManager::new(store.clone(), f.config(), f.access());
    let initial = jobs
        .start("alpha", JobOperation::Crawl, 2, true, false)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if jobs.status(&initial.id).await.unwrap().job.status == JobStatus::Completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    f.hold.store(true, Ordering::SeqCst);
    let (first, second) = tokio::join!(
        jobs.start("alpha", JobOperation::Recrawl, 2, true, false),
        jobs.start("alpha", JobOperation::Recrawl, 2, true, false)
    );
    assert_ne!(
        first.is_ok(),
        second.is_ok(),
        "exactly one simultaneous source admission must succeed"
    );
    let job = first.or(second).unwrap();
    tokio::time::timeout(Duration::from_secs(5), f.entered.notified())
        .await
        .unwrap(); // Admission returned while HTTP is still blocked.
    assert!(
        jobs.start("alpha", JobOperation::Recrawl, 2, true, false)
            .await
            .is_err()
    );
    let other = jobs
        .start("beta", JobOperation::Crawl, 2, true, false)
        .await
        .unwrap();
    assert!(
        jobs.start("beta", JobOperation::Crawl, 2, true, false)
            .await
            .is_err()
    );
    assert_eq!(
        jobs.cancel(&job.id).await.unwrap().job.status,
        JobStatus::Cancelled
    );
    assert_eq!(
        jobs.cancel(&job.id).await.unwrap().job.status,
        JobStatus::Cancelled
    );
    jobs.shutdown().await;
    assert_eq!(
        jobs.status(&other.id).await.unwrap().job.status,
        JobStatus::Cancelled
    );
    assert!(
        jobs.start("alpha", JobOperation::Crawl, 2, true, false)
            .await
            .is_err()
    );
    drop(jobs);
    drop(public);
    drop(denied);
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    let status = store.get_job(&job.id).await.unwrap().unwrap();
    assert_eq!(status.job.status, JobStatus::Cancelled);
    assert_eq!(status.run.unwrap().status, "cancelled");
    assert_eq!(
        store.source_status("alpha").await.unwrap()["active_documents"],
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stdio_eof_cancels_jobs_and_process_crash_recovers_interrupted_jobs() {
    let f = Fixture::start().await;
    let directory = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(directory.path().join("knowledge"))
        .await
        .unwrap();
    f.sources(&store).await;
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    f.hold.store(true, Ordering::SeqCst);
    for crash in [false, true] {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_Ariadne"))
            .arg("mcp")
            .env("ARIADNE_DATA_DIR", directory.path())
            .env("ARIADNE_MCP_CRAWL_SOURCES", "alpha")
            .env("ARIADNE_MCP_ALLOW_PRIVATE_NETWORK", "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let client =
            ().serve((child.stdout.take().unwrap(), child.stdin.take().unwrap()))
                .await
                .unwrap();
        let job = client
            .call_tool(arguments(
                "crawl",
                json!({"source_id":"alpha","lexical_only":true,"max_pages":2}),
            ))
            .await
            .unwrap()
            .structured_content
            .unwrap();
        let id = job["job"]["id"].as_str().unwrap();
        tokio::time::timeout(Duration::from_secs(10), f.entered.notified())
            .await
            .unwrap();
        if crash {
            child.kill().await.unwrap();
            let _ = client.cancel().await;
        } else {
            client.cancel().await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_secs(15), child.wait())
                    .await
                    .unwrap()
                    .unwrap()
                    .success()
            );
        }
        let store = KnowledgeStore::open(directory.path().join("knowledge"))
            .await
            .unwrap();
        let job = store.get_job(id).await.unwrap().unwrap();
        assert_eq!(
            job.job.status,
            if crash {
                JobStatus::Interrupted
            } else {
                JobStatus::Cancelled
            }
        );
        assert_eq!(
            job.run.unwrap().status,
            if crash { "interrupted" } else { "cancelled" }
        );
        drop(store);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

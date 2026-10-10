//! Crawl real fixture HTTP, restart embedded storage, speak MCP with the official client.
use ariadne::{
    crawler::{CrawlRequest, CrawlScope},
    embeddings::{OllamaConfig, OllamaProvider},
    ingestion::{IngestionStatus, ingest, ingest_lexical},
    storage::{KnowledgeStore, Source},
};
use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    task::JoinHandle,
};
use url::Url;

struct Fixture {
    root: Url,
    fail: Arc<AtomicBool>,
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
        let fail = Arc::new(AtomicBool::new(false));
        let flag = fail.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let flag = flag.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let (end, length) = loop {
                        let mut buffer = [0; 4096];
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&buffer[..n]);
                        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                            let length = String::from_utf8_lossy(&bytes[..end])
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|n| n.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if bytes.len() >= end + 4 + length {
                                break (end + 4, length);
                            }
                        }
                    };
                    let path = String::from_utf8_lossy(&bytes)
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .to_owned();
                    let (status, kind, body) = match path.as_str() {
                        "/robots.txt" => (200, "text/plain", "User-agent: *\nAllow: /\n".into()),
                        "/docs/" => (200, "text/html", "<html><title>Fixture documentation</title><main><h1>Overview</h1><p>Documentation for HTTP networking.</p><a href='/docs/proxy'>Proxy</a><a href='/docs/pool'>Pool</a></main></html>".into()),
                        "/docs/proxy" => (200, "text/html", "<html><title>Proxy configuration</title><main><h1 id='proxy'>Proxy configuration</h1><p>Route HTTP requests through a SOCKS proxy.</p><pre><code>Proxy::all(\"socks5://localhost:1080\")</code></pre></main></html>".into()),
                        "/docs/pool" => (200, "text/html", "<html><title>Connection pool</title><main><h1>Connection pool</h1><p>Reuse connections to reduce latency.</p></main></html>".into()),
                        "/api/tags" => (200, "application/json", json!({"models":[{"name":"fixture:latest","digest":"a".repeat(64)}]}).to_string()),
                        "/api/embed" if flag.load(Ordering::SeqCst) => (400, "application/json", "provider unavailable".into()),
                        "/api/embed" => {
                            let request: Value = serde_json::from_slice(&bytes[end..end+length]).unwrap();
                            assert_eq!(request["truncate"], false);
                            let vectors: Vec<_> = request["input"].as_array().unwrap().iter().map(|v| {
                                let text = v.as_str().unwrap().to_lowercase();
                                if text.contains("socks") || text.contains("proxy::all") || text.contains("intermediary") { vec![1., 0., 0.] }
                                else if text.contains("pool") { vec![0., 1., 0.] } else { vec![0., 0., 1.] }
                            }).collect();
                            (200, "application/json", json!({"model":"fixture:latest", "embeddings":vectors}).to_string())
                        }
                        _ => (404, "text/plain", "missing".into()),
                    };
                    let reply = format!(
                        "HTTP/1.1 {status} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket.write_all(reply.as_bytes()).await.unwrap();
                });
            }
        });
        Self { root, fail, task }
    }
    fn config(&self) -> OllamaConfig {
        OllamaConfig {
            endpoint: self.root.clone(),
            model: "fixture".into(),
        }
    }
    fn source(&self) -> Source {
        Source::new("docs", "Fixture", self.root.join("docs/").unwrap()).unwrap()
    }
    fn request(&self, id: &str) -> CrawlRequest {
        let url = self.source().root_url;
        let mut request = CrawlRequest::new(
            "docs",
            id,
            url.clone(),
            CrawlScope::new(url, "/docs").unwrap(),
        );
        request.max_pages = 3;
        request
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_ingestion_restart_and_real_mcp_client_search_both_modes() {
    acceptance(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an explicitly started local Ollama and embeddinggemma model"]
async fn real_ollama_ingestion_restart_and_mcp_semantic_search() {
    acceptance(true).await;
}

async fn acceptance(real_model: bool) {
    let fixture = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let endpoint = if real_model {
        std::env::var("ARIADNE_OLLAMA_URL").unwrap_or_else(|_| "http://127.0.0.1:11434/".into())
    } else {
        fixture.root.to_string()
    };
    let model = if real_model {
        "embeddinggemma:latest"
    } else {
        "fixture"
    };
    let command = |args: &[&str]| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_Ariadne"));
        command
            .args(args)
            .env("ARIADNE_DATA_DIR", dir.path())
            .env("ARIADNE_OLLAMA_URL", &endpoint)
            .env("ARIADNE_EMBED_MODEL", model);
        command
    };
    let output = command(&[
        "source",
        "add",
        "docs",
        "Fixture",
        fixture.source().root_url.as_str(),
    ])
    .output()
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = command(&["ingest", "docs", "first", "--max-pages", "3"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let run: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(run["ingestion"]["status"], "completed");
    assert_eq!(run["summary"]["document_count"], 3);
    assert_eq!(run["ingestion"]["embedding_coverage"]["ready"], 3);

    let hybrid = command(&[
        "hybrid-search",
        "Proxy::all",
        "--source",
        "docs",
        "--limit",
        "1",
    ])
    .output()
    .await
    .unwrap();
    assert!(
        hybrid.status.success(),
        "{}",
        String::from_utf8_lossy(&hybrid.stderr)
    );
    let hybrid: Value = serde_json::from_slice(&hybrid.stdout).unwrap();
    assert_eq!(hybrid[0]["match_kind"], "hybrid");
    let context = command(&[
        "retrieve",
        "Proxy::all",
        "--source",
        "docs",
        "--limit",
        "1",
        "--context-chars",
        "1000",
    ])
    .output()
    .await
    .unwrap();
    assert!(
        context.status.success(),
        "{}",
        String::from_utf8_lossy(&context.stderr)
    );
    let context: Value = serde_json::from_slice(&context.stdout).unwrap();
    assert_eq!(context["passages"].as_array().unwrap().len(), 1);
    assert!(context["total_text_chars"].as_u64().unwrap() <= 1000);
    let transport = TokioChildProcess::new(command(&["mcp"])).unwrap();
    let client = ().serve(transport).await.unwrap();
    let tools = client.list_tools(None).await.unwrap();
    assert_eq!(tools.tools.len(), 13);
    assert_eq!(tools.tools[0].name, "search");
    let no_lexical = client
        .call_tool(
            CallToolRequestParams::new("search")
                .with_arguments(json!({"query":"intermediary"}).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(no_lexical.structured_content.unwrap()["hits"], json!([]));
    for (mode, query) in [
        ("lexical", "Proxy::all"),
        ("hybrid", "Proxy::all"),
        (
            "vector",
            "How can I send HTTP traffic through an intermediary server?",
        ),
    ] {
        let result = client.call_tool(CallToolRequestParams::new("search").with_arguments(
            json!({"query":query,"mode":mode,"source_id":"docs","limit":1,"max_text_chars":60}).as_object().unwrap().clone()
        )).await.unwrap();
        assert_ne!(result.is_error, Some(true));
        let data = result.structured_content.unwrap();
        assert_eq!(data["mode"], mode);
        let hit = &data["hits"][0];
        assert_eq!(hit["content_kind"], "source_data");
        assert_eq!(hit["source_id"], "docs");
        assert_eq!(hit["crawl_id"], "first");
        assert!(
            hit["url"].as_str().unwrap().contains("/docs/proxy"),
            "{mode} ranked unexpected hit: {hit}"
        );
        assert!(hit["text"].as_str().unwrap().chars().count() <= 60);
        assert!(!hit["heading_path"].as_array().unwrap().is_empty());
        assert!(hit["crawled_at"].is_object());
        assert!(hit["score"].is_number());
        assert!(!hit["chunk_id"].as_str().unwrap().is_empty());
        assert_eq!(result.content.len(), 1); // JSON text compatibility for older clients.
    }
    let expanded = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(
                json!({"query":"Proxy::all","mode":"hybrid","source_id":"docs",
            "filter":{"url_prefix":fixture.root.join("docs/proxy").unwrap().as_str()},
            "context":{"max_total_chars":1000}})
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .unwrap();
    assert_ne!(expanded.is_error, Some(true));
    let expanded = expanded.structured_content.unwrap();
    assert_eq!(expanded["hits"][0]["match_kind"], "hybrid");
    assert!(expanded["hits"][0]["fusion"]["lexical_rank"].is_number());
    assert_eq!(expanded["context"]["passages"].as_array().unwrap().len(), 1);
    assert!(expanded["context"]["total_text_chars"].as_u64().unwrap() <= 1000);
    assert_eq!(
        expanded["context"]["passages"][0]["chunks"][0]["content_kind"],
        "source_data"
    );
    for args in [
        json!({"query":"proxy","limit":51}),
        json!({"query":"proxy","sql":"SELECT *"}),
        json!({"query":"proxy","mode":"graph"}),
        json!({"query":"proxy","context":{"neighbor_chunks":4}}),
        json!({"query":"proxy","filter":{"url_prefix":"file:///etc/passwd"}}),
        json!({"query":"proxy","filter":{"unknown":true}}),
    ] {
        assert!(
            client
                .call_tool(
                    CallToolRequestParams::new("search")
                        .with_arguments(args.as_object().unwrap().clone())
                )
                .await
                .is_err()
        );
    }
    let empty = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(
                json!({"query":"proxy","source_id":"other"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(empty.structured_content.unwrap()["hits"], json!([]));
    if !real_model {
        fixture.fail.store(true, Ordering::SeqCst);
        let failed = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    json!({"query":"intermediary","mode":"vector"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(failed.is_error, Some(true));
    }
    let lexical = client
        .call_tool(
            CallToolRequestParams::new("search")
                .with_arguments(json!({"query":"proxy"}).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        lexical.structured_content.unwrap()["hits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    client.cancel().await.unwrap();
    if !real_model {
        // A fresh server starts with a failing provider: lexical use must not
        // require a successful Ollama handshake or dimension probe.
        let transport = TokioChildProcess::new(command(&["mcp"])).unwrap();
        let client = ().serve(transport).await.unwrap();
        let lexical = client
            .call_tool(
                CallToolRequestParams::new("search")
                    .with_arguments(json!({"query":"Proxy::all"}).as_object().unwrap().clone()),
            )
            .await
            .unwrap();
        assert_eq!(
            lexical.structured_content.unwrap()["hits"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let vector = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    json!({"query":"intermediary","mode":"vector"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(vector.is_error, Some(true));
        client.cancel().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedding_connection_failure_keeps_committed_text_and_audits_partial_success() {
    let fixture = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    let result = ingest::<OllamaProvider, _, _>(
        &store,
        fixture.source(),
        fixture.request("failure"),
        Default::default(),
        || async { anyhow::bail!("fixture provider offline") },
    )
    .await;
    assert!(result.is_err());
    let run = store.get_crawl("docs", "failure").await.unwrap().unwrap();
    assert_eq!(run.status, "completed");
    let progress = run.ingestion.unwrap();
    assert_eq!(progress.status, IngestionStatus::Partial);
    assert!(progress.error.unwrap().contains("provider offline"));
    assert_eq!(
        ariadne::retrieval::search(&store, ariadne::retrieval::SearchQuery::new("Proxy::all"))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedding_failures_checkpoint_coverage_and_source_embeddings_can_resume() {
    let fixture = Fixture::start().await;
    let provider = OllamaProvider::connect(fixture.config()).await.unwrap();
    fixture.fail.store(true, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    let run = ingest(
        &store,
        fixture.source(),
        fixture.request("partial"),
        Default::default(),
        || async { Ok(provider) },
    )
    .await
    .unwrap();
    let progress = run.ingestion.unwrap();
    assert_eq!(progress.status, IngestionStatus::Partial);
    assert_eq!(progress.embedding_report.unwrap().failed, 3);
    assert_eq!(progress.embedding_coverage.unwrap().failed, 3);
    fixture.fail.store(false, Ordering::SeqCst);
    let provider = OllamaProvider::connect(fixture.config()).await.unwrap();
    assert_eq!(
        ariadne::embeddings::index_source(&store, &provider, "docs")
            .await
            .unwrap()
            .generated,
        3
    );
    // Original run progress is historical; current coverage reflects retries.
    assert_eq!(
        store
            .embedding_coverage(
                ariadne::embeddings::EmbeddingProvider::space(&provider),
                "docs"
            )
            .await
            .unwrap()
            .ready,
        3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_embedding_stage_is_recovered_after_restart() {
    let fixture = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KnowledgeStore::open(dir.path().join("db")).await.unwrap());
    let worker_store = store.clone();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let source = fixture.source();
    let request = fixture.request("interrupted");
    let task = tokio::spawn(async move {
        ingest::<OllamaProvider, _, _>(
            &worker_store,
            source,
            request,
            Default::default(),
            || async {
                entered.send(()).unwrap();
                std::future::pending().await
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), ready)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    let run = store
        .get_crawl("docs", "interrupted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "completed");
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Interrupted);
}

#[tokio::test]
async fn rejected_fetches_are_partial_and_invalid_budgets_create_no_run() {
    let fixture = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("db")).await.unwrap();
    let mut request = fixture.request("invalid");
    request.max_pages = 0;
    assert!(
        ingest_lexical(&store, fixture.source(), request, Default::default())
            .await
            .is_err()
    );
    assert!(store.get_crawl("docs", "invalid").await.unwrap().is_none());
    let source = Source::new("missing", "Missing", fixture.root.join("missing").unwrap()).unwrap();
    let request = CrawlRequest::new(
        "missing",
        "404",
        source.root_url.clone(),
        CrawlScope::new(source.root_url.clone(), "/missing").unwrap(),
    );
    let run = ingest_lexical(&store, source, request, Default::default())
        .await
        .unwrap();
    assert_eq!(run.ingestion.unwrap().status, IngestionStatus::Partial);
    assert_eq!(run.summary.unwrap()["rejected_count"], 1);
    assert_eq!(
        store.page_outcomes("missing", "404").await.unwrap().len(),
        1
    );
}

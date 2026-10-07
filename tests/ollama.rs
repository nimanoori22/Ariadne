//! Real HTTP adapter exercised with a deterministic local protocol fixture.

#[tokio::test]
async fn runner_variants_use_the_selected_manifest_and_first_load_changes_are_reprobed() {
    for mode in [10, 12] {
        let server = Server::start().await;
        server.mode.store(mode, Ordering::SeqCst);
        let provider = server.provider().await;
        assert_eq!(provider.space().revision, "b".repeat(64));
        if mode == 12 {
            assert_eq!(server.calls.load(Ordering::SeqCst), 2);
        }
        embed_checked(&provider, &["hello".into()], EmbeddingPurpose::Query)
            .await
            .unwrap();
        assert_eq!(provider.space().revision, "b".repeat(64));
    }
    let server = Server::start().await;
    server.mode.store(11, Ordering::SeqCst);
    let result = OllamaProvider::connect(OllamaConfig {
        endpoint: server.url.clone(),
        model: "test-model".into(),
    })
    .await;
    assert!(result.is_err());
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_or_oversized_response_bodies_are_rejected_without_leaking_content() {
    let server = Server::start().await;
    let provider = server.provider().await;
    for mode in [8, 9] {
        server.mode.store(mode, Ordering::SeqCst);
        let error = embed_checked(&provider, &["hello".into()], EmbeddingPurpose::Query)
            .await
            .unwrap_err();
        assert!(!format!("{error:#}").contains("SECRET"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_embeds_resumes_reports_coverage_and_returns_vector_hits() {
    use ariadne::{
        crawler::{CrawlRequest, CrawlScope, PageOutcome, PageState},
        extraction::extract,
        ingestion::ExtractionBatch,
        storage::{KnowledgeStore, Source},
    };
    use std::{
        process::Command,
        time::{Duration, UNIX_EPOCH},
    };
    let server = Server::start().await;
    let dir = tempfile::TempDir::new().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    let url = Url::parse("https://example.test/docs/proxy").unwrap();
    store
        .register_source(Source::new("docs", "Fixture docs", url.clone()).unwrap())
        .await
        .unwrap();
    store
        .begin_crawl(&CrawlRequest::new(
            "docs",
            "initial",
            url.clone(),
            CrawlScope::new(url.clone(), "/docs").unwrap(),
        ))
        .await
        .unwrap();
    let page = extract(PageOutcome { source_id: "docs".into(), crawl_id: "initial".into(), requested_url: url.to_string(), final_url: url.to_string(), fetched_at: UNIX_EPOCH, status: 200, headers: vec![], raw_body: b"<main><h1 id='proxy'>Proxy</h1><p>Proxy configuration routes HTTP requests.</p></main>".to_vec(), content_truncated: false, state: PageState::Fetched });
    store
        .finish_crawl(ExtractionBatch {
            source_id: "docs".into(),
            crawl_id: "initial".into(),
            started_at: UNIX_EPOCH,
            finished_at: UNIX_EPOCH,
            outcomes: vec![page],
            blocked: vec![],
            dropped_pages: 0,
            audit_overflow: false,
            discovery: Default::default(),
        })
        .await
        .unwrap();
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let command = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_Ariadne"))
            .env("ARIADNE_DATA_DIR", dir.path())
            .env("ARIADNE_OLLAMA_URL", server.url.as_str())
            .env("ARIADNE_EMBED_MODEL", "test-model")
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    assert_eq!(command(&["embed", "docs"])["generated"], 1);
    assert_eq!(command(&["embed", "docs"])["reused"], 1);
    let status = command(&["embeddings", "status", "docs"]);
    assert_eq!(status[0]["coverage"]["ready"], 1);
    assert_eq!(status[0]["coverage"]["missing"], 0);
    let hits = command(&[
        "vector-search",
        "intermediary",
        "--source",
        "docs",
        "--limit",
        "1",
        "--max-chars",
        "16",
    ]);
    assert_eq!(hits.as_array().unwrap().len(), 1);
    assert_eq!(hits[0]["url"], "https://example.test/docs/proxy#proxy");
    assert_eq!(hits[0]["match_kind"], "vector");
    assert_eq!(hits[0]["embedding_space"]["dimensions"], 3);
    assert_eq!(hits[0]["text"].as_str().unwrap().chars().count(), 16);
    assert_eq!(hits[0]["text_truncated"], true);
    let calls = server.calls.load(Ordering::SeqCst);
    let invalid = Command::new(env!("CARGO_BIN_EXE_Ariadne"))
        .env("ARIADNE_DATA_DIR", dir.path())
        .env("ARIADNE_OLLAMA_URL", server.url.as_str())
        .env("ARIADNE_EMBED_MODEL", "test-model")
        .args(["vector-search", "query", "--limit", "0"])
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    assert_eq!(server.calls.load(Ordering::SeqCst), calls);
}
use ariadne::embeddings::{
    EmbeddingProvider, EmbeddingPurpose, OllamaConfig, OllamaProvider, embed_checked,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
use url::Url;

struct Server {
    url: Url,
    mode: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<Value>>>,
    handle: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let mode = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let (m, c, b) = (mode.clone(), calls.clone(), bodies.clone());
        let handle = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                let (header_end, length) = loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break (0, 0);
                    }
                    request.extend_from_slice(&buffer[..n]);
                    assert!(request.len() < 100_000);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&request[..end]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.to_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|n| n.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (end + 4, length);
                    }
                };
                if header_end == 0 {
                    continue;
                }
                while request.len() < header_end + length {
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..n]);
                }
                let is_tags = request.starts_with(b"GET /api/tags");
                let is_show = request.starts_with(b"POST /api/show");
                let mode = m.load(Ordering::SeqCst);
                let (status, body, extra) = if is_tags {
                    let drift = (mode == 4 && c.load(Ordering::SeqCst) >= 2)
                        || (mode == 12 && c.load(Ordering::SeqCst) >= 1);
                    let mut models = vec![
                        json!({"name": "test-model:latest", "digest": if drift { "b".repeat(64) } else { "a".repeat(64) }}),
                    ];
                    if [10, 11].contains(&mode) {
                        models.push(json!({"name": "test-model:latest", "digest": "b".repeat(64)}));
                        if c.load(Ordering::SeqCst) % 2 == 1 {
                            models.reverse();
                        }
                    }
                    ("200 OK", json!({"models": models}).to_string(), "")
                } else if is_show {
                    ("200 OK", json!({"manifests": [{"digest": format!("sha256:{}", "a".repeat(64))}, {"digest": format!("sha256:{}", "b".repeat(64)), "selected": mode != 11}]}).to_string(), "")
                } else {
                    assert!(request.starts_with(b"POST /api/embed"));
                    let body: Value =
                        serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
                    assert_eq!(body["truncate"], false);
                    assert_eq!(body["model"], "test-model:latest");
                    let count = body["input"].as_array().unwrap().len();
                    b.lock().unwrap().push(body);
                    let call = c.fetch_add(1, Ordering::SeqCst) + 1;
                    if mode == 1 && call <= 2 {
                        (
                            "429 Too Many Requests",
                            "retry".into(),
                            "Retry-After: 0\r\n",
                        )
                    } else if mode == 2 {
                        (
                            "400 Bad Request",
                            "SECRET source contents echoed by server".into(),
                            "",
                        )
                    } else if mode == 7 {
                        (
                            "503 Service Unavailable",
                            "busy".into(),
                            "Retry-After: 999\r\n",
                        )
                    } else if mode == 8 {
                        ("200 OK", json!({"model": "test-model:latest", "embeddings": [["SECRET provider echo"]]}).to_string(), "")
                    } else if mode == 9 {
                        ("200 OK", "x".repeat(4_000_001), "")
                    } else {
                        let vectors = if mode == 3 {
                            vec![vec![1.0]; count]
                        } else if mode == 5 {
                            vec![]
                        } else {
                            vec![vec![3.0, 4.0, 0.0]; count]
                        };
                        let response_model = if mode == 6 {
                            "different:latest"
                        } else {
                            "test-model:latest"
                        };
                        (
                            "200 OK",
                            json!({"model": response_model, "embeddings": vectors}).to_string(),
                            "",
                        )
                    }
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        Self {
            url,
            mode,
            calls,
            bodies,
            handle,
        }
    }
    async fn provider(&self) -> OllamaProvider {
        OllamaProvider::connect(OllamaConfig {
            endpoint: self.url.clone(),
            model: "test-model".into(),
        })
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn resolves_revision_probes_dimension_batches_and_disables_truncation() {
    let server = Server::start().await;
    let provider = server.provider().await;
    assert_eq!(provider.space().dimensions, 3);
    assert_eq!(provider.space().revision, "a".repeat(64));
    assert_eq!(provider.space().model, "test-model:latest");
    let vectors = embed_checked(
        &provider,
        &["first".into(), "second".into()],
        EmbeddingPurpose::Document,
    )
    .await
    .unwrap();
    assert_eq!(vectors, [vec![0.6, 0.8, 0.0], vec![0.6, 0.8, 0.0]]);
    let bodies = server.bodies.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[1]["input"], json!(["first", "second"]));
}

#[tokio::test]
async fn retries_transient_status_with_a_bounded_attempt_count() {
    let server = Server::start().await;
    let provider = server.provider().await;
    server.calls.store(0, Ordering::SeqCst);
    server.mode.store(1, Ordering::SeqCst);
    embed_checked(&provider, &["hello".into()], EmbeddingPurpose::Query)
        .await
        .unwrap();
    assert_eq!(server.calls.load(Ordering::SeqCst), 3);
    server.mode.store(7, Ordering::SeqCst);
    let error = embed_checked(&provider, &["hello".into()], EmbeddingPurpose::Query)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Retry-After"));
    assert_eq!(server.calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn permanent_error_body_is_not_exposed_and_input_cap_prevents_a_request() {
    let server = Server::start().await;
    let provider = server.provider().await;
    server.mode.store(2, Ordering::SeqCst);
    let error = embed_checked(&provider, &["hello".into()], EmbeddingPurpose::Query)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("400"));
    assert!(!format!("{error:#}").contains("SECRET"));
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    assert!(
        embed_checked(&provider, &["x".repeat(8193)], EmbeddingPurpose::Document)
            .await
            .is_err()
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn model_drift_response_model_and_vector_shape_are_rejected() {
    for mode in [3, 4, 5, 6] {
        let server = Server::start().await;
        let provider = server.provider().await;
        server.mode.store(mode, Ordering::SeqCst);
        assert!(
            embed_checked(&provider, &["hello".into()], EmbeddingPurpose::Query)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn missing_model_is_an_actionable_error() {
    let server = Server::start().await;
    let result = OllamaProvider::connect(OllamaConfig {
        endpoint: server.url.clone(),
        model: "not-installed".into(),
    })
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("expected missing model error"),
    };
    assert!(format!("{error:#}").contains("not installed"));
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
}

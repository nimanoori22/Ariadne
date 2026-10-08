use ariadne::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, CrawlScope, PageOutcome, PageState},
    embeddings::OllamaConfig,
    extraction::extract,
    ingestion::{ExtractionBatch, prepare_crawl},
    mcp::KnowledgeMcp,
    storage::{KnowledgeStore, Source},
};
use rmcp::{ServiceExt, model::CallToolRequestParams};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use url::Url;

fn url(path: &str) -> Url {
    Url::parse(&format!("https://example.test/docs/{path}")).unwrap()
}
async fn commit(store: &KnowledgeStore, source: &str, run: &str, pages: &[(&str, &str)]) {
    let root = url("");
    store
        .begin_crawl(&CrawlRequest::new(
            source,
            run,
            root.clone(),
            CrawlScope::new(root, "/docs").unwrap(),
        ))
        .await
        .unwrap();
    let now = SystemTime::now();
    let outcomes = pages
        .iter()
        .map(|(path, html)| {
            extract(PageOutcome {
                source_id: source.into(),
                crawl_id: run.into(),
                requested_url: url(path).to_string(),
                final_url: url(path).to_string(),
                fetched_at: now,
                status: 200,
                headers: vec![],
                raw_body: html.as_bytes().to_vec(),
                content_truncated: false,
                rendering: None,
                state: PageState::Fetched,
            })
        })
        .collect();
    store
        .finish_prepared_crawl(
            prepare_crawl(
                ExtractionBatch {
                    source_id: source.into(),
                    crawl_id: run.into(),
                    started_at: now,
                    finished_at: now,
                    outcomes,
                    blocked: vec![],
                    dropped_pages: 0,
                    audit_overflow: false,
                    discovery: Default::default(),
                },
                ChunkPolicy { target_chars: 70 },
            )
            .unwrap(),
        )
        .await
        .unwrap();
}
async fn setup() -> (TempDir, KnowledgeStore) {
    let dir = TempDir::new().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    for source in ["docs", "other"] {
        store
            .register_source(Source::new(source, source, url("")).unwrap())
            .await
            .unwrap();
    }
    (dir, store)
}
const CLIENT: &str = "<title>Client</title><nav><a href='noise'>Ignore</a></nav><main><h1 id='client'>Client</h1><p>ProseOnly::not_an_entity and explanation.</p><p><code>reqwest::Proxy</code></p><p>Separate chunk with <code>Widget::new</code>.</p><p><a href='proxy#setup'>Proxy setup</a><a href='proxy#setup'>Duplicate</a><a href='missing?keep=1#anchor'>Missing</a><a href='https://elsewhere.test/api'>External</a><a href='mailto:a@example.test'>Email</a></p></main>";
const PROXY: &str = "<title>Proxy</title><main><h1 id='setup'>Proxy</h1><pre><code>reqwest::Proxy</code></pre></main>";

#[tokio::test]
async fn one_hop_links_and_exact_entities_are_source_backed_bounded_and_durable() {
    let (dir, store) = setup().await;
    // Origin arrives before target: unresolved edge later resolves without recrawl.
    commit(&store, "docs", "one", &[("client", CLIENT)]).await;
    let before = store
        .document_links("docs", &url("client"), false, 100)
        .await
        .unwrap();
    assert_eq!(before["links"].as_array().unwrap().len(), 3, "{before}");
    assert!(
        before["links"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["target_indexed"] == false)
    );
    commit(&store, "docs", "two", &[("proxy", PROXY)]).await;
    commit(&store, "other", "other-one", &[("client", CLIENT)]).await;
    let outgoing = store
        .document_links("docs", &url("client#ignored"), false, 100)
        .await
        .unwrap();
    let proxy = outgoing["links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["target_url"] == url("proxy").as_str())
        .unwrap();
    assert_eq!(proxy["target_indexed"], true);
    assert_eq!(proxy["target_availability"], "active");
    assert_eq!(proxy["url"], url("proxy#setup").as_str());
    assert_eq!(proxy["source_id"], "docs");
    assert_eq!(proxy["crawl_id"], "one");
    assert!(proxy["crawled_at"].is_object());
    let incoming = store
        .document_links("docs", &url("proxy"), true, 20)
        .await
        .unwrap();
    assert_eq!(incoming["links"].as_array().unwrap().len(), 1);
    assert_eq!(incoming["links"][0]["document_url"], url("client").as_str());
    assert!(
        store
            .document_links("other", &url("proxy"), true, 20)
            .await
            .unwrap()["links"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let bounded = store
        .document_links("docs", &url("client"), false, 1)
        .await
        .unwrap();
    assert_eq!(bounded["links"].as_array().unwrap().len(), 1);
    assert_eq!(bounded["truncated"], true);
    let mentions = store
        .entity_mentions("docs", "reqwest::Proxy", 100)
        .await
        .unwrap();
    assert_eq!(
        mentions["mentions"].as_array().unwrap().len(),
        2,
        "{mentions}"
    );
    assert_eq!(mentions["coverage"]["documents"], 2);
    assert_eq!(mentions["coverage"]["indexed_documents"], 2);
    for mention in mentions["mentions"].as_array().unwrap() {
        assert_eq!(mention["content_kind"], "source_data");
        assert_eq!(mention["source_id"], "docs");
        let chunks = store
            .get_chunks(
                "docs",
                &Url::parse(mention["document_url"].as_str().unwrap()).unwrap(),
            )
            .await
            .unwrap();
        let chunk = chunks
            .iter()
            .find(|c| c.id == mention["chunk_id"].as_str().unwrap())
            .unwrap();
        assert_eq!(mention["section_id"], chunk.section_id);
        assert_eq!(mention["url"], chunk.source_url.as_str());
        assert!(chunk.text.contains("reqwest::Proxy"));
    }
    assert!(
        store
            .entity_mentions("docs", "ProseOnly::not_an_entity", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .entity_mentions("docs", "reqwest::proxy", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .entity_mentions("docs", "reqwest::Proxy", 1)
            .await
            .unwrap()["truncated"],
        true
    );
    assert!(store.entity_mentions("docs", "Proxy", 20).await.is_err());
    assert!(
        store
            .entity_mentions("docs", "reqwest::Proxy", 101)
            .await
            .is_err()
    );
    drop(store);
    let mut reopened = None;
    for _ in 0..100 {
        match KnowledgeStore::open(dir.path().join("knowledge")).await {
            Ok(store) => {
                reopened = Some(store);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let store = reopened.unwrap();
    assert_eq!(
        store
            .document_links("docs", &url("client"), false, 100)
            .await
            .unwrap(),
        outgoing
    );
    assert_eq!(
        store
            .entity_mentions("docs", "reqwest::Proxy", 100)
            .await
            .unwrap(),
        mentions
    );
    commit(
        &store,
        "docs",
        "replace",
        &[(
            "client",
            "<main><h1>New</h1><p><code>Different::new</code></p></main>",
        )],
    )
    .await;
    assert!(
        store
            .document_links("docs", &url("proxy"), true, 20)
            .await
            .unwrap()["links"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let remaining = store
        .entity_mentions("docs", "reqwest::Proxy", 20)
        .await
        .unwrap();
    assert_eq!(remaining["mentions"].as_array().unwrap().len(), 1);
    assert_eq!(
        remaining["mentions"][0]["document_url"],
        url("proxy").as_str()
    );
    assert_eq!(
        store
            .entity_mentions("other", "reqwest::Proxy", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // Duplicate ingestion is idempotent; unrelated sources stay isolated.
    commit(
        &store,
        "docs",
        "same",
        &[(
            "client",
            "<main><h1>New</h1><p><code>Different::new</code></p></main>",
        )],
    )
    .await;
    assert_eq!(
        store
            .entity_mentions("docs", "Different::new", 20)
            .await
            .unwrap()["mentions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

fn call(name: &str, args: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_owned()).with_arguments(args.as_object().unwrap().clone())
}
#[tokio::test]
async fn real_mcp_exposes_graph_reads_and_rejects_invalid_inputs() {
    let (_dir, store) = setup().await;
    commit(
        &store,
        "docs",
        "initial",
        &[("client", CLIENT), ("proxy", PROXY)],
    )
    .await;
    let (a, b) = tokio::io::duplex(65536);
    let server = tokio::spawn(
        KnowledgeMcp::new(
            store,
            OllamaConfig {
                endpoint: Url::parse("http://127.0.0.1:1/").unwrap(),
                model: "unused".into(),
            },
        )
        .serve(a),
    );
    let client = ().serve(b).await.unwrap();
    let server = server.await.unwrap().unwrap();
    let tools = client.list_tools(None).await.unwrap();
    for name in ["get_links", "find_entity"] {
        assert!(tools.tools.iter().any(|t| t.name == name));
    }
    let incoming = client
        .call_tool(call(
            "get_links",
            json!({"source_id":"docs","url":url("proxy"),"incoming":true}),
        ))
        .await
        .unwrap();
    assert_ne!(incoming.is_error, Some(true), "{incoming:?}");
    assert_eq!(
        incoming.structured_content.unwrap()["links"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let mentions = client
        .call_tool(call(
            "find_entity",
            json!({"source_id":"docs","entity":"reqwest::Proxy","limit":1}),
        ))
        .await
        .unwrap();
    assert_eq!(mentions.structured_content.unwrap()["truncated"], true);
    for (name, args) in [
        (
            "get_links",
            json!({"source_id":"docs","url":"file:///etc/passwd"}),
        ),
        (
            "get_links",
            json!({"source_id":"docs","url":url("client"),"limit":0}),
        ),
        (
            "find_entity",
            json!({"source_id":"docs","entity":"reqwest::Proxy(); THROW 'bad'"}),
        ),
        (
            "find_entity",
            json!({"source_id":"docs","entity":"reqwest::Proxy","unknown":true}),
        ),
    ] {
        assert!(client.call_tool(call(name, args)).await.is_err());
    }
    client.cancel().await.unwrap();
    server.cancel().await.unwrap();
}

#[tokio::test]
async fn standalone_cli_reads_graph_after_ingestion_restart() {
    let (dir, store) = setup().await;
    commit(
        &store,
        "docs",
        "cli",
        &[("client", CLIENT), ("proxy", PROXY)],
    )
    .await;
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let run = |args: &[&str]| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_Ariadne"))
            .env("ARIADNE_DATA_DIR", dir.path())
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
    let links = run(&["links", "docs", url("proxy").as_str(), "incoming"]);
    assert_eq!(links["links"].as_array().unwrap().len(), 1);
    let mentions = run(&["entity", "docs", "reqwest::Proxy"]);
    assert_eq!(mentions["mentions"].as_array().unwrap().len(), 2);
    assert_eq!(mentions["coverage"]["indexed_documents"], 2);
}

use ariadne::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, CrawlScope, PageOutcome, PageState},
    extraction::extract,
    ingestion::{ExtractionBatch, prepare_crawl},
    retrieval::{
        ContextOptions, RevisionSelector, SearchQuery, assemble_context, search, search_revision,
    },
    storage::{KnowledgeStore, Source},
};
use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};
use url::Url;

const A: &str = "<title>Client</title><main><h1 id='client'>Client</h1><p>Shared paragraph about clients.</p><h2 id='old'>Old API</h2><p>AncientMarker café 東京 <code>Old::configure</code>.</p><pre><code>Old::configure();</code></pre></main>";
const B: &str = "<title>Client</title><main><h1 id='client'>Client</h1><p>Shared paragraph about clients.</p><h2 id='new'>New API</h2><p>ModernMarker <code>New::configure</code>.</p></main>";
fn url() -> Url {
    Url::parse("https://example.test/docs/client").unwrap()
}
async fn save(store: &KnowledgeStore, source: &str, run: &str, html: &str, policy: ChunkPolicy) {
    let url = url();
    let request = CrawlRequest::new(
        source,
        run,
        url.clone(),
        CrawlScope::new(url.clone(), "/docs").unwrap(),
    );
    store.begin_crawl(&request).await.unwrap();
    let now = SystemTime::now();
    let page = PageOutcome {
        source_id: source.into(),
        crawl_id: run.into(),
        requested_url: url.to_string(),
        final_url: url.to_string(),
        fetched_at: now,
        status: 200,
        headers: vec![],
        raw_body: html.as_bytes().to_vec(),
        content_truncated: false,
        rendering: None,
        state: PageState::Fetched,
    };
    let batch = ExtractionBatch {
        source_id: source.into(),
        crawl_id: run.into(),
        started_at: now,
        finished_at: now,
        outcomes: vec![extract(page)],
        blocked: vec![],
        dropped_pages: 0,
        audit_overflow: false,
        discovery: Default::default(),
    };
    store
        .finish_prepared_crawl(prepare_crawl(batch, policy).unwrap())
        .await
        .unwrap();
}
async fn open(path: &std::path::Path) -> KnowledgeStore {
    for _ in 0..100 {
        match KnowledgeStore::open(path).await {
            Ok(store) => return store,
            Err(_) => tokio::time::sleep(Duration::from_millis(30)).await,
        }
    }
    panic!("database did not close")
}
fn selector(id: &str) -> RevisionSelector {
    RevisionSelector {
        source_id: "docs".into(),
        document_url: url(),
        revision_id: id.into(),
    }
}
async fn revisions(store: &KnowledgeStore) -> Vec<Value> {
    store
        .list_revisions("docs", &url(), None, 100)
        .await
        .unwrap()["revisions"]
        .as_array()
        .unwrap()
        .clone()
}
async fn fixture() -> (tempfile::TempDir, KnowledgeStore, String) {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    store
        .register_source(Source::new("docs", "Docs", url()).unwrap())
        .await
        .unwrap();
    save(&store, "docs", "first", A, Default::default()).await;
    let id = revisions(&store).await[0]["revision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    save(&store, "docs", "second", B, Default::default()).await;
    (dir, store, id)
}

#[tokio::test]
async fn a_to_b_restart_preserves_raw_structure_chunks_and_latest_search() {
    let (dir, store, id) = fixture().await;
    let before = revisions(&store).await;
    assert_eq!(before.len(), 2);
    assert_eq!(before[0]["is_current"], false);
    assert_eq!(before[1]["is_current"], true);
    assert_eq!(before[0]["crawl_id"], "first");
    assert_ne!(
        before[0]["indexing"]["hashes"]["normalized_sha256"],
        before[1]["indexing"]["hashes"]["normalized_sha256"]
    );
    assert_eq!(before[0]["revision_id"].as_str().unwrap().len(), 64);
    let original = store.get_revision(&selector(&id)).await.unwrap().unwrap();
    assert_eq!(original.page.raw_body, A.as_bytes());
    assert!(original.markdown().contains("Old::configure"));
    assert!(
        original
            .sections
            .iter()
            .any(|s| s.anchor.as_deref() == Some("old"))
    );
    let hits = search_revision(&store, SearchQuery::new("Old::configure"), selector(&id))
        .await
        .unwrap();
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .all(|h| h.revision_id.as_deref() == Some(&id) && h.crawl_id == "first")
    );
    assert!(
        search(&store, SearchQuery::new("AncientMarker"))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !search(&store, SearchQuery::new("ModernMarker"))
            .await
            .unwrap()
            .is_empty()
    );
    drop(store);
    let store = open(&dir.path().join("knowledge")).await;
    assert_eq!(revisions(&store).await, before);
    assert_eq!(
        serde_json::to_value(store.get_revision(&selector(&id)).await.unwrap()).unwrap(),
        serde_json::to_value(Some(original)).unwrap()
    );
    let context = assemble_context(&store, &hits, ContextOptions::default())
        .await
        .unwrap();
    assert_eq!(context.skipped_stale_hits, 0);
    assert!(
        context
            .passages
            .iter()
            .all(|p| p.revision_id.as_deref() == Some(&id))
    );
    assert!(
        context
            .passages
            .iter()
            .flat_map(|p| &p.chunks)
            .all(|c| c.crawl_id == "first"
                && c.revision_id.as_deref() == Some(&id)
                && !c.text.contains("ModernMarker"))
    );
    let page = store.list_revisions("docs", &url(), None, 1).await.unwrap();
    assert_eq!(page["revisions"][0], before[0]);
    let next = store
        .list_revisions("docs", &url(), page["next_cursor"].as_u64(), 1)
        .await
        .unwrap();
    assert_eq!(next["revisions"][0], before[1]);
    assert!(next["next_cursor"].is_null());
    assert!(store.list_revisions("docs", &url(), None, 0).await.is_err());
    let mut filtered = SearchQuery::new("AncientMarker");
    filtered.filter.heading = Some("New API".into());
    assert!(
        search_revision(&store, filtered, selector(&id))
            .await
            .unwrap()
            .is_empty()
    );
    let full = store
        .knowledge_revision(&selector(&id), None, 20000)
        .await
        .unwrap()
        .unwrap();
    let section = full["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["text"].as_str().unwrap().contains("AncientMarker"))
        .unwrap()["section_id"]
        .as_u64()
        .unwrap() as usize;
    let bounded = store
        .knowledge_revision(&selector(&id), Some(section), 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bounded["text_chars"], 7);
    assert_eq!(bounded["truncated"], true);
    assert!(
        store
            .knowledge_revision(&selector(&id), Some(9999), 4000)
            .await
            .unwrap()
            .is_none()
    );
    let mut wrong = selector(&id);
    wrong.source_id = "other".into();
    assert!(store.get_revision(&wrong).await.unwrap().is_none());
    assert!(
        store
            .knowledge_revision(&wrong, None, 4000)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        search_revision(&store, SearchQuery::new("AncientMarker"), wrong)
            .await
            .unwrap()
            .is_empty()
    );
    let mut wrong = selector(&id);
    wrong.document_url = url().join("other").unwrap();
    assert!(store.get_revision(&wrong).await.unwrap().is_none());
    assert!(
        search_revision(&store, SearchQuery::new("AncientMarker"), wrong)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .knowledge_revision(&selector("bad"), None, 4000)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn stable_revisions_revert_and_policy_changes_and_context_does_not_merge_versions() {
    let (_dir, store, id) = fixture().await;
    let old = revisions(&store).await[0].clone();
    let mut historic = search_revision(&store, SearchQuery::new("Shared paragraph"), selector(&id))
        .await
        .unwrap();
    let live = search(&store, SearchQuery::new("Shared paragraph"))
        .await
        .unwrap();
    assert_eq!(historic[0].chunk_id, live[0].chunk_id);
    historic.extend(live);
    let context = assemble_context(&store, &historic, ContextOptions::default())
        .await
        .unwrap();
    assert_eq!(context.passages.len(), 2);
    assert_eq!(context.deduplicated_chunks, 0);
    assert!(context.passages.iter().any(|p| p.revision_id.is_none()));
    save(&store, "docs", "revert", A, Default::default()).await;
    let revs = revisions(&store).await;
    assert_eq!(revs.len(), 2);
    assert_eq!(revs[0]["is_current"], true);
    assert_eq!(revs[0]["crawl_id"], "first");
    assert_eq!(revs[0]["stored_at"], old["stored_at"]);
    assert_eq!(revs[0]["indexing"], old["indexing"]);
    assert_eq!(
        store
            .get_revision(&selector(&id))
            .await
            .unwrap()
            .unwrap()
            .page
            .crawl_id,
        "first"
    );
    assert!(
        search(&store, SearchQuery::new("AncientMarker"))
            .await
            .unwrap()
            .iter()
            .all(|h| h.crawl_id == "revert")
    );
    save(&store, "docs", "same", A, Default::default()).await;
    assert_eq!(revisions(&store).await.len(), 2);
    save(
        &store,
        "docs",
        "policy",
        A,
        ChunkPolicy { target_chars: 40 },
    )
    .await;
    let revs = revisions(&store).await;
    assert_eq!(revs.len(), 3);
    assert_eq!(
        revs[2]["indexing"]["hashes"]["normalized_sha256"],
        old["indexing"]["hashes"]["normalized_sha256"]
    );
    assert_ne!(revs[2]["revision_id"], id);
    assert_eq!(revs[2]["is_current"], true);
    store
        .register_source(Source::new("other", "Other", url()).unwrap())
        .await
        .unwrap();
    save(&store, "other", "other", A, Default::default()).await;
    let other = store
        .list_revisions("other", &url(), None, 20)
        .await
        .unwrap();
    assert_ne!(other["revisions"][0]["revision_id"], id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_and_fresh_stdio_mcp_read_and_search_explicit_history() {
    let (dir, store, id) = fixture().await;
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let command = |args: &[&str]| {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_Ariadne"));
        cmd.env("ARIADNE_DATA_DIR", dir.path()).args(args);
        cmd
    };
    for (args, key) in [
        (
            vec!["revisions", "docs", url().as_str(), "--limit", "1"],
            "revisions",
        ),
        (vec!["revision", "docs", url().as_str(), &id], "chunks"),
        (
            vec![
                "revision-retrieve",
                "docs",
                url().as_str(),
                &id,
                "AncientMarker",
                "--retrieval-mode",
                "lexical",
            ],
            "passages",
        ),
    ] {
        let output = command(&args).output().await.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(!value[key].as_array().unwrap().is_empty());
    }
    let client = ().serve(TokioChildProcess::new(command(&["mcp"])).unwrap()).await.unwrap();
    let call = |name: &str, args: Value| {
        CallToolRequestParams::new(name.to_owned())
            .with_arguments(args.as_object().unwrap().clone())
    };
    let tools = client.list_tools(None).await.unwrap();
    assert!(tools.tools.iter().any(|t| t.name == "list_revisions"));
    let list = client
        .call_tool(call(
            "list_revisions",
            json!({"source_id":"docs","url":url(),"limit":1}),
        ))
        .await
        .unwrap();
    assert_ne!(list.is_error, Some(true), "{list:?}");
    assert_eq!(
        list.structured_content.unwrap()["revisions"][0]["revision_id"],
        id
    );
    let doc = client
        .call_tool(call(
            "get_document",
            json!({"source_id":"docs","url":url(),"revision_id":id}),
        ))
        .await
        .unwrap();
    assert_ne!(doc.is_error, Some(true), "{doc:?}");
    let doc = doc.structured_content.unwrap();
    assert_eq!(doc["metadata"]["revision_id"], id);
    assert!(doc.to_string().contains("AncientMarker"));
    let hit = client
        .call_tool(call(
            "search",
            json!({"query":"AncientMarker","revision":selector(&id),"context":{}}),
        ))
        .await
        .unwrap();
    assert_ne!(hit.is_error, Some(true), "{hit:?}");
    let hit = hit.structured_content.unwrap();
    assert_eq!(hit["hits"][0]["revision_id"], id);
    assert_eq!(hit["context"]["passages"][0]["revision_id"], id);
    let latest = client
        .call_tool(call("search", json!({"query":"AncientMarker"})))
        .await
        .unwrap();
    assert_eq!(latest.structured_content.unwrap()["hits"], json!([]));
    for args in [
        json!({"query":"AncientMarker","mode":"vector","revision":selector(&id)}),
        json!({"query":"AncientMarker","graph":{},"revision":selector(&id)}),
        json!({"query":"AncientMarker","source_id":"other","revision":selector(&id)}),
    ] {
        assert!(client.call_tool(call("search", args)).await.is_err());
    }
    assert!(
        client
            .call_tool(call(
                "get_document",
                json!({"source_id":"docs","url":url(),"revision_id":"bad"})
            ))
            .await
            .is_err()
    );
    client.cancel().await.unwrap();
    // The test does not start or connect to Ollama for historical reads.
}

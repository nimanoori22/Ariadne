use ariadne::{
    crawler::{CrawlRequest, CrawlScope, PageOutcome, PageState},
    extraction::{ExtractionOutcome, extract},
    ingestion::ExtractionBatch,
    storage::{KnowledgeStore, Source},
};
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use url::Url;

#[test]
fn standalone_cli_initializes_and_reopens_without_database_tools() {
    let dir = TempDir::new().unwrap();
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
        output.stdout
    };
    run(&[
        "source",
        "add",
        "docs",
        "Docs ' ; THROW 'data stays data",
        "https://example.test/docs/",
    ]);
    let sources: serde_json::Value = serde_json::from_slice(&run(&["source", "list"])).unwrap();
    assert_eq!(sources[0]["id"], "docs");
    assert_eq!(sources[0]["name"], "Docs ' ; THROW 'data stays data");
    assert!(dir.path().join("knowledge").is_dir());
}

fn url() -> Url {
    Url::parse("https://example.test/docs/client").unwrap()
}
fn request(run: &str) -> CrawlRequest {
    CrawlRequest::new("docs", run, url(), CrawlScope::new(url(), "/docs").unwrap())
}
fn page(run: &str, html: &str) -> PageOutcome {
    PageOutcome {
        source_id: "docs".into(),
        crawl_id: run.into(),
        requested_url: url().to_string(),
        final_url: url().to_string(),
        fetched_at: SystemTime::now(),
        status: 200,
        headers: vec![("content-type".into(), b"text/html; charset=utf-8".to_vec())],
        raw_body: html.as_bytes().to_vec(),
        content_truncated: false,
        state: PageState::Fetched,
    }
}
fn batch(run: &str, outcomes: Vec<ExtractionOutcome>) -> ExtractionBatch {
    let now = SystemTime::now();
    ExtractionBatch {
        source_id: "docs".into(),
        crawl_id: run.into(),
        started_at: now,
        finished_at: now,
        outcomes,
        blocked: vec![],
        dropped_pages: 0,
        audit_overflow: false,
    }
}
async fn setup() -> (TempDir, KnowledgeStore) {
    let dir = TempDir::new().unwrap();
    let store = KnowledgeStore::open(dir.path().join("database"))
        .await
        .unwrap();
    store
        .register_source(Source::new("docs", "Documentation", url()).unwrap())
        .await
        .unwrap();
    (dir, store)
}
async fn reopen(path: &std::path::Path) -> KnowledgeStore {
    // Dropping the SDK handle signals asynchronous engine shutdown.
    for _ in 0..100 {
        match KnowledgeStore::open(path).await {
            Ok(store) => return store,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("embedded database did not release its directory");
}

#[tokio::test]
async fn schema_and_full_document_survive_reopen() {
    let (dir, store) = setup().await;
    let source = store.get_source("docs").await.unwrap().unwrap();
    let again = store
        .register_source(Source::new("docs", "Documentation", url()).unwrap())
        .await
        .unwrap();
    assert_eq!(source, again);
    assert!(
        store
            .register_source(Source::new("docs", "Conflicting", url()).unwrap())
            .await
            .is_err()
    );
    store.begin_crawl(&request("first")).await.unwrap();
    let html = include_str!("fixtures/documentation.html");
    let expected = extract(page("first", html));
    let expected = match expected {
        ExtractionOutcome::Extracted(doc) => doc,
        _ => panic!("fixture rejected"),
    };
    let raw = expected.page.raw_body.clone();
    let markdown = expected.markdown();
    let sections = serde_json::to_value(&expected.sections).unwrap();
    store
        .finish_crawl(batch("first", vec![ExtractionOutcome::Extracted(expected)]))
        .await
        .unwrap();
    let doc = store.get_document("docs", &url()).await.unwrap().unwrap();
    assert_eq!(doc.markdown(), markdown);
    assert_eq!(serde_json::to_value(&doc.sections).unwrap(), sections);
    let indexing = store.get_indexing("docs", &url()).await.unwrap().unwrap();
    let chunks = store.get_chunks("docs", &url()).await.unwrap();
    assert_eq!(indexing.chunk_count, chunks.len());
    assert!(!chunks.is_empty());
    let path = dir.path().join("database");
    drop(store);
    let store = reopen(&path).await;
    assert_eq!(store.list_sources().await.unwrap(), vec![source]);
    let doc = store.get_document("docs", &url()).await.unwrap().unwrap();
    assert_eq!(doc.page.raw_body, raw);
    assert_eq!(doc.markdown(), markdown);
    assert_eq!(
        store.get_indexing("docs", &url()).await.unwrap().unwrap(),
        indexing
    );
    assert_eq!(store.get_chunks("docs", &url()).await.unwrap(), chunks);
    assert_eq!(
        store
            .get_crawl("docs", "first")
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    assert_eq!(store.page_outcomes("docs", "first").await.unwrap().len(), 1);
}

#[tokio::test]
async fn replacement_removes_old_sections_and_rejections_keep_good_data() {
    let (_dir, store) = setup().await;
    store.begin_crawl(&request("first")).await.unwrap();
    store
        .finish_crawl(batch(
            "first",
            vec![extract(page(
                "first",
                "<main><h1>Client</h1><h2>Old</h2><p>Old content</p></main>",
            ))],
        ))
        .await
        .unwrap();
    store.begin_crawl(&request("second")).await.unwrap();
    store
        .finish_crawl(batch(
            "second",
            vec![extract(page(
                "second",
                "<main><h1>Client</h1><p>New content</p></main>",
            ))],
        ))
        .await
        .unwrap();
    let doc = store.get_document("docs", &url()).await.unwrap().unwrap();
    assert_eq!(doc.sections.len(), 2);
    assert!(!doc.markdown().contains("Old"));
    let before = serde_json::to_value(doc).unwrap();
    let chunks_before = store.get_chunks("docs", &url()).await.unwrap();
    store.begin_crawl(&request("third")).await.unwrap();
    let mut failed = page("third", "server unavailable");
    failed.status = 503;
    failed.state = PageState::HttpFailure;
    let mut rejected = batch("third", vec![extract(failed)]);
    rejected.dropped_pages = 1;
    store.finish_crawl(rejected).await.unwrap();
    assert_eq!(
        serde_json::to_value(store.get_document("docs", &url()).await.unwrap().unwrap()).unwrap(),
        before
    );
    let audit = store.page_outcomes("docs", "third").await.unwrap();
    assert_eq!(
        store.get_chunks("docs", &url()).await.unwrap(),
        chunks_before
    );
    assert_eq!(audit[0]["reason"], "FetchNotSuccessful");
    assert_eq!(audit[0]["page"]["status"], 503);
    let run = store.get_crawl("docs", "third").await.unwrap().unwrap();
    assert_eq!(run.summary.unwrap()["delivery_complete"], false);
}

#[tokio::test]
async fn bad_batch_cannot_complete_run_or_replace_document() {
    let (_dir, store) = setup().await;
    store.begin_crawl(&request("first")).await.unwrap();
    assert!(store.begin_crawl(&request("first")).await.is_err());
    assert!(
        store
            .begin_crawl(&CrawlRequest::new(
                "unknown",
                "run",
                url(),
                CrawlScope::new(url(), "/docs").unwrap()
            ))
            .await
            .is_err()
    );
    let doc = extract(page(
        "other-run",
        "<main><h1>Wrong</h1><p>content</p></main>",
    ));
    assert!(store.finish_crawl(batch("first", vec![doc])).await.is_err());
    assert!(store.get_document("docs", &url()).await.unwrap().is_none());
    assert!(
        store
            .page_outcomes("docs", "first")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .get_crawl("docs", "first")
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    store
        .fail_crawl("docs", "first", "test failure")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_crawl("docs", "first")
            .await
            .unwrap()
            .unwrap()
            .error
            .as_deref(),
        Some("test failure")
    );
    assert!(store.finish_crawl(batch("first", vec![])).await.is_err());
}

#[tokio::test]
async fn second_process_is_excluded_and_restart_marks_running_crawl_interrupted() {
    let (dir, store) = setup().await;
    let path = dir.path().join("database");
    assert!(KnowledgeStore::open(&path).await.is_err());
    store.begin_crawl(&request("interrupted")).await.unwrap();
    drop(store);
    let store = reopen(&path).await;
    assert_eq!(
        store
            .get_crawl("docs", "interrupted")
            .await
            .unwrap()
            .unwrap()
            .status,
        "interrupted"
    );
}

#[tokio::test]
async fn identical_redirect_aliases_share_document_and_preserve_both_audits() {
    let (_dir, store) = setup().await;
    store.begin_crawl(&request("aliases")).await.unwrap();
    let html = "<main><h1>Client</h1><p>Shared content</p></main>";
    let mut alias = page("aliases", html);
    alias.requested_url = "https://example.test/docs/redirect".into();
    store
        .finish_crawl(batch(
            "aliases",
            vec![extract(alias), extract(page("aliases", html))],
        ))
        .await
        .unwrap();
    assert_eq!(
        store.page_outcomes("docs", "aliases").await.unwrap().len(),
        2
    );
    let doc = store.get_document("docs", &url()).await.unwrap().unwrap();
    assert_eq!(doc.page.requested_url, url().as_str());
    let summary = store
        .get_crawl("docs", "aliases")
        .await
        .unwrap()
        .unwrap()
        .summary
        .unwrap();
    assert_eq!(summary["document_count"], 1);
    assert_eq!(summary["alias_count"], 1);
    assert_eq!(summary["rejected_count"], 0);
    store.begin_crawl(&request("conflict")).await.unwrap();
    assert!(
        store
            .finish_crawl(batch(
                "conflict",
                vec![
                    extract(page("conflict", html)),
                    extract(page(
                        "conflict",
                        "<main><h1>Changed</h1><p>Different content</p></main>"
                    ))
                ]
            ))
            .await
            .is_err()
    );
    assert!(
        store
            .page_outcomes("docs", "conflict")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .get_document("docs", &url())
            .await
            .unwrap()
            .unwrap()
            .page
            .crawl_id,
        "aliases"
    );
}

#[tokio::test]
async fn recrawl_is_idempotent_and_changed_document_removes_obsolete_chunks() {
    use ariadne::{chunking::ChunkPolicy, ingestion::prepare_crawl};
    let (_dir, store) = setup().await;
    let html = "<main><h1 id='client'>Client</h1><p>First explanation about clients.</p><p>Second explanation about clients.</p><h2 id='proxy'>Proxy</h2><p>Old proxy content.</p></main>";
    let policy = ChunkPolicy { target_chars: 60 };
    store.begin_crawl(&request("original")).await.unwrap();
    store
        .finish_prepared_crawl(
            prepare_crawl(
                batch("original", vec![extract(page("original", html))]),
                policy,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let original = store.get_chunks("docs", &url()).await.unwrap();
    assert_eq!(original.len(), 3);
    let metadata = store.get_indexing("docs", &url()).await.unwrap().unwrap();
    store.begin_crawl(&request("same")).await.unwrap();
    store
        .finish_prepared_crawl(
            prepare_crawl(batch("same", vec![extract(page("same", html))]), policy).unwrap(),
        )
        .await
        .unwrap();
    let same = store.get_chunks("docs", &url()).await.unwrap();
    assert_eq!(
        same.iter().map(|chunk| &chunk.id).collect::<Vec<_>>(),
        original.iter().map(|chunk| &chunk.id).collect::<Vec<_>>()
    );
    assert_eq!(
        store.get_indexing("docs", &url()).await.unwrap().unwrap(),
        metadata
    );
    assert!(same.iter().all(|chunk| chunk.crawl_id == "same"));
    store.begin_crawl(&request("changed")).await.unwrap();
    store.finish_prepared_crawl(prepare_crawl(batch("changed", vec![extract(page("changed", "<main><h1 id='client'>Client</h1><p>First explanation about clients.</p><h2 id='proxy'>Proxy</h2><p>New proxy content.</p></main>"))]), policy).unwrap()).await.unwrap();
    let changed = store.get_chunks("docs", &url()).await.unwrap();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].id, original[0].id);
    assert!(
        changed
            .iter()
            .all(|chunk| chunk.id != original[1].id && chunk.id != original[2].id)
    );
    assert_ne!(
        store
            .get_indexing("docs", &url())
            .await
            .unwrap()
            .unwrap()
            .hashes
            .normalized_sha256,
        metadata.hashes.normalized_sha256
    );
}

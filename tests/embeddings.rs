use anyhow::Result;
use ariadne::{
    crawler::{CrawlRequest, CrawlScope, PageOutcome, PageState},
    embeddings::{
        EmbeddingProvider, EmbeddingPurpose, EmbeddingSpace, EmbeddingStatus, ProviderLimits,
        embed_checked, index_source,
    },
    extraction::extract,
    ingestion::ExtractionBatch,
    retrieval::{ContentKind, MatchKind, SearchQuery, search, vector_search},
    storage::{KnowledgeStore, Source},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};
use url::Url;

struct TestProvider {
    space: EmbeddingSpace,
    calls: Arc<AtomicUsize>,
    fault: usize,
}
impl TestProvider {
    fn new(revision: &str, dimensions: usize) -> Self {
        Self {
            space: EmbeddingSpace {
                provider: "fixture".into(),
                model: "semantic-test".into(),
                revision: revision.into(),
                dimensions,
                input_version: "fixture-v1".into(),
            },
            calls: Arc::new(AtomicUsize::new(0)),
            fault: 0,
        }
    }
}
impl EmbeddingProvider for TestProvider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }
    fn limits(&self) -> ProviderLimits {
        ProviderLimits {
            batch_size: 2,
            max_input_bytes: 5000,
            max_batch_bytes: 8000,
        }
    }
    async fn embed(&self, inputs: &[String], _purpose: EmbeddingPurpose) -> Result<Vec<Vec<f64>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fault == 1 {
            anyhow::bail!("fixture outage");
        }
        let mut vectors = Vec::new();
        for input in inputs {
            if input.contains("RejectMe") {
                anyhow::bail!("fixture input rejection");
            }
            let lower = input.to_lowercase();
            let vector = if lower.contains("proxy") || lower.contains("intermediary") {
                vec![1.0, 0.05, 0.0]
            } else if lower.contains("timeout") || lower.contains("duration") {
                vec![0.0, 1.0, 0.05]
            } else {
                vec![0.0, 0.05, 1.0]
            };
            let vector = if self.space.dimensions == 2 {
                vector[..2].to_vec()
            } else {
                vector
            };
            vectors.push(match self.fault {
                2 => vec![1.0],
                3 => vec![0.0; self.space.dimensions],
                4 => vec![f64::NAN; self.space.dimensions],
                _ => vector,
            });
        }
        if self.fault == 5 {
            vectors.clear();
        }
        Ok(vectors)
    }
}

async fn ingest(store: &KnowledgeStore, source: &str, run: &str, pages: &[(&str, &str)]) {
    let root = Url::parse(&format!("https://example.test/{source}/")).unwrap();
    store
        .register_source(Source::new(source, format!("{source} docs"), root.clone()).unwrap())
        .await
        .unwrap();
    store
        .begin_crawl(&CrawlRequest::new(
            source,
            run,
            root.clone(),
            CrawlScope::new(root.clone(), root.path()).unwrap(),
        ))
        .await
        .unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(123);
    let outcomes = pages
        .iter()
        .map(|(path, html)| {
            let url = root.join(path).unwrap().to_string();
            extract(PageOutcome {
                source_id: source.into(),
                crawl_id: run.into(),
                requested_url: url.clone(),
                final_url: url,
                fetched_at: now,
                status: 200,
                headers: vec![],
                raw_body: html.as_bytes().to_vec(),
                content_truncated: false,
                state: PageState::Fetched,
            })
        })
        .collect();
    store
        .finish_crawl(ExtractionBatch {
            source_id: source.into(),
            crawl_id: run.into(),
            started_at: now,
            finished_at: now,
            outcomes,
            blocked: vec![],
            dropped_pages: 0,
            audit_overflow: false,
            discovery: Default::default(),
        })
        .await
        .unwrap();
}

async fn fixture() -> (tempfile::TempDir, KnowledgeStore) {
    let dir = tempfile::TempDir::new().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    ingest(&store, "rust", "initial", &[
        ("proxy", "<title>Proxy</title><main><h1 id='socks'>SOCKS proxies</h1><p>Proxy::custom routes requests through a proxy.</p></main>"),
        ("timeout", "<main><h1 id='time'>Timeout</h1><p>A timeout bounds request duration.</p></main>"),
        ("cookies", "<main><h1>Cookies</h1><p>Cookie jars hold state.</p></main>"),
    ]).await;
    (dir, store)
}

#[tokio::test]
async fn semantic_search_keeps_provenance_and_reuses_vectors_across_restart() {
    let (dir, store) = fixture().await;
    let provider = TestProvider::new("revision1", 3);
    let report = index_source(&store, &provider, "rust").await.unwrap();
    assert_eq!((report.scanned, report.generated, report.failed), (3, 3, 0));
    let coverage = store
        .embedding_coverage(&provider.space, "rust")
        .await
        .unwrap();
    assert_eq!(
        (coverage.current_chunks, coverage.ready, coverage.missing),
        (3, 3, 0)
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    let mut query = SearchQuery::new("route traffic through an intermediary");
    query.limit = 1;
    assert!(search(&store, query.clone()).await.unwrap().is_empty());
    let hits = vector_search(&store, &provider, query.clone())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let hit = &hits[0];
    assert_eq!(hit.title, "Proxy");
    assert_eq!(hit.match_kind, MatchKind::Vector);
    assert_eq!(hit.content_kind, ContentKind::SourceData);
    assert_eq!(hit.embedding_space.as_ref(), Some(&provider.space));
    assert_eq!(hit.url.as_str(), "https://example.test/rust/proxy#socks");
    assert_eq!(hit.crawl_id, "initial");
    assert_eq!(hit.crawled_at, UNIX_EPOCH + Duration::from_secs(123));
    assert_eq!(hit.source_name, "rust docs");
    assert_eq!(hit.heading_path, ["SOCKS proxies"]);
    assert_eq!(hit.content_sha256.len(), 64);
    assert!(hit.score > 0.99);
    let first_states = store
        .embedding_states(&provider.space, "rust")
        .await
        .unwrap();
    assert!(
        first_states
            .iter()
            .all(|s| s.status == EmbeddingStatus::Ready
                && s.attempts == 1
                && s.created_at.is_some())
    );
    let before = provider.calls.load(Ordering::SeqCst);
    assert_eq!(
        index_source(&store, &provider, "rust")
            .await
            .unwrap()
            .reused,
        3
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    assert_eq!(vector_search(&store, &provider, query).await.unwrap(), hits);
    let mut outage = TestProvider::new("revision1", 3);
    outage.fault = 1;
    assert_eq!(
        index_source(&store, &outage, "rust").await.unwrap().reused,
        3
    );
    assert_eq!(outage.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.embedding_states(&outage.space, "rust").await.unwrap()[0].created_at,
        first_states[0].created_at
    );
}

#[tokio::test]
async fn filtered_knn_fills_the_requested_slots_from_the_selected_source() {
    let (_dir, store) = fixture().await;
    // Many nearer vectors in another source must not occupy the selected
    // source's K slots before filtering is applied.
    let distractors: Vec<(String, String)> = (0..24)
        .map(|i| {
            (
                format!("proxy{i}"),
                format!("<main><h1>Proxy {i}</h1><p>proxy transport</p></main>"),
            )
        })
        .collect();
    let refs: Vec<(&str, &str)> = distractors
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    ingest(&store, "other", "initial", &refs).await;
    let provider = TestProvider::new("revision1", 3);
    index_source(&store, &provider, "rust").await.unwrap();
    index_source(&store, &provider, "other").await.unwrap();
    let mut query = SearchQuery::new("intermediary transport");
    query.source_id = Some("rust".into());
    query.limit = 3;
    let hits = vector_search(&store, &provider, query.clone())
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);
    assert!(hits.iter().all(|h| h.source_id == "rust"));
    assert_eq!(hits[0].title, "Proxy");
    query.source_id = Some("absent".into());
    assert!(
        vector_search(&store, &provider, query)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn different_revisions_and_dimensions_have_isolated_indexes() {
    let (_dir, store) = fixture().await;
    let first = TestProvider::new("revision1", 3);
    let second = TestProvider::new("revision2", 2);
    assert_eq!(
        store
            .embedding_coverage(&second.space, "rust")
            .await
            .unwrap()
            .missing,
        3
    );
    index_source(&store, &first, "rust").await.unwrap();
    assert!(
        vector_search(&store, &second, SearchQuery::new("intermediary"))
            .await
            .is_err()
    );
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    index_source(&store, &second, "rust").await.unwrap();
    assert_ne!(first.space.id(), second.space.id());
    assert_eq!(store.list_embedding_spaces().await.unwrap().len(), 2);
    let hits = vector_search(&store, &second, SearchQuery::new("intermediary"))
        .await
        .unwrap();
    assert!(
        hits.iter()
            .all(|h| h.embedding_space.as_ref() == Some(&second.space))
    );
    assert_eq!(
        index_source(&store, &first, "rust").await.unwrap().reused,
        3
    );
}

#[tokio::test]
async fn provider_failures_are_checkpointed_and_retryable_without_affecting_lexical_search() {
    let (_dir, store) = fixture().await;
    let mut provider = TestProvider::new("revision1", 3);
    provider.fault = 1;
    let report = index_source(&store, &provider, "rust").await.unwrap();
    assert_eq!((report.generated, report.failed), (0, 3));
    let states = store
        .embedding_states(&provider.space, "rust")
        .await
        .unwrap();
    assert!(states.iter().all(|s| s.status == EmbeddingStatus::Failed
        && s.error.as_deref().unwrap().contains("fixture outage")));
    assert_eq!(
        search(&store, SearchQuery::new("Proxy::custom"))
            .await
            .unwrap()
            .len(),
        1
    );
    provider.fault = 0;
    assert_eq!(
        index_source(&store, &provider, "rust")
            .await
            .unwrap()
            .generated,
        3
    );
    assert!(
        store
            .embedding_states(&provider.space, "rust")
            .await
            .unwrap()
            .iter()
            .all(|s| s.status == EmbeddingStatus::Ready && s.attempts == 2)
    );
}

#[tokio::test]
async fn malformed_vectors_never_enter_the_index() {
    for fault in 2..=5 {
        let mut provider = TestProvider::new("revision1", 3);
        provider.fault = fault;
        assert!(
            embed_checked(&provider, &["proxy".into()], EmbeddingPurpose::Query)
                .await
                .is_err()
        );
    }
    let (_dir, store) = fixture().await;
    let mut provider = TestProvider::new("revision1", 3);
    provider.fault = 2;
    assert_eq!(
        index_source(&store, &provider, "rust")
            .await
            .unwrap()
            .failed,
        3
    );
    provider.fault = 0;
    assert!(
        vector_search(&store, &provider, SearchQuery::new("intermediary"))
            .await
            .unwrap()
            .is_empty()
    );
    let mut query = SearchQuery::new("intermediary");
    query.limit = 0;
    let before = provider.calls.load(Ordering::SeqCst);
    assert!(vector_search(&store, &provider, query).await.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn input_rejection_is_isolated_and_oversized_content_stays_intact() {
    let (_dir, store) = fixture().await;
    let huge = format!(
        "<main><h1>Large example</h1><pre><code>{}</code></pre></main>",
        "x".repeat(6000)
    );
    ingest(
        &store,
        "rust",
        "more",
        &[
            (
                "bad",
                "<main><h1>Rejected</h1><p>RejectMe input is deliberately rejected.</p></main>",
            ),
            ("large", &huge),
        ],
    )
    .await;
    let provider = TestProvider::new("revision1", 3);
    let report = index_source(&store, &provider, "rust").await.unwrap();
    assert_eq!((report.generated, report.failed), (3, 2));
    let large = store
        .get_chunks(
            "rust",
            &Url::parse("https://example.test/rust/large").unwrap(),
        )
        .await
        .unwrap();
    assert!(large[0].markdown.contains(&"x".repeat(6000)));
    assert_eq!(large[0].token_count, None);
    let states = store
        .embedding_states(&provider.space, "rust")
        .await
        .unwrap();
    assert_eq!(
        states
            .iter()
            .filter(|s| s.status == EmbeddingStatus::Failed)
            .count(),
        2
    );
}

#[tokio::test]
async fn recrawl_reuses_unchanged_vectors_and_invalidates_obsolete_vectors_atomically() {
    let (_dir, store) = fixture().await;
    let provider = TestProvider::new("revision1", 3);
    index_source(&store, &provider, "rust").await.unwrap();
    let original = store
        .get_chunks(
            "rust",
            &Url::parse("https://example.test/rust/proxy").unwrap(),
        )
        .await
        .unwrap();
    ingest(&store, "rust", "unchanged", &[("proxy", "<title>Proxy</title><main><h1 id='socks'>SOCKS proxies</h1><p>Proxy::custom routes requests through a proxy.</p></main>")]).await;
    assert_eq!(
        index_source(&store, &provider, "rust")
            .await
            .unwrap()
            .reused,
        3
    );
    let hits = vector_search(&store, &provider, SearchQuery::new("intermediary"))
        .await
        .unwrap();
    assert_eq!(hits[0].crawl_id, "unchanged");
    ingest(
        &store,
        "rust",
        "changed",
        &[(
            "proxy",
            "<main><h1>New timeout</h1><p>Timeout limits request duration instead.</p></main>",
        )],
    )
    .await;
    let states = store
        .embedding_states(&provider.space, "rust")
        .await
        .unwrap();
    assert_eq!(
        states
            .iter()
            .find(|s| s.chunk_id == original[0].id)
            .unwrap()
            .status,
        EmbeddingStatus::Stale
    );
    assert!(
        vector_search(&store, &provider, SearchQuery::new("intermediary"))
            .await
            .unwrap()
            .iter()
            .all(|h| h.chunk_id != original[0].id)
    );
    let report = index_source(&store, &provider, "rust").await.unwrap();
    assert_eq!((report.generated, report.reused), (1, 2));
}

#[tokio::test]
async fn cancelling_generation_leaves_pending_work_that_can_resume() {
    struct Hanging(TestProvider);
    impl EmbeddingProvider for Hanging {
        fn space(&self) -> &EmbeddingSpace {
            self.0.space()
        }
        fn limits(&self) -> ProviderLimits {
            self.0.limits()
        }
        async fn embed(&self, _: &[String], _: EmbeddingPurpose) -> Result<Vec<Vec<f64>>> {
            std::future::pending().await
        }
    }
    let (_dir, store) = fixture().await;
    let hanging = Hanging(TestProvider::new("revision1", 3));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(1500),
            index_source(&store, &hanging, "rust")
        )
        .await
        .is_err()
    );
    let states = store
        .embedding_states(hanging.space(), "rust")
        .await
        .unwrap();
    assert!(!states.is_empty());
    assert!(states.iter().all(|s| s.status == EmbeddingStatus::Pending));
    let provider = TestProvider::new("revision1", 3);
    assert_eq!(
        index_source(&store, &provider, "rust")
            .await
            .unwrap()
            .generated,
        3
    );
}

#[tokio::test]
async fn a_document_replaced_during_inference_cannot_receive_the_old_vectors() {
    use tokio::sync::Notify;
    struct Paused {
        inner: TestProvider,
        started: Notify,
        resume: Notify,
        calls: AtomicUsize,
    }
    impl EmbeddingProvider for Paused {
        fn space(&self) -> &EmbeddingSpace {
            self.inner.space()
        }
        fn limits(&self) -> ProviderLimits {
            self.inner.limits()
        }
        async fn embed(
            &self,
            inputs: &[String],
            purpose: EmbeddingPurpose,
        ) -> Result<Vec<Vec<f64>>> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                self.resume.notified().await;
            }
            self.inner.embed(inputs, purpose).await
        }
    }
    let (_dir, store) = fixture().await;
    let provider = Paused {
        inner: TestProvider::new("revision1", 3),
        started: Notify::new(),
        resume: Notify::new(),
        calls: AtomicUsize::new(0),
    };
    let indexing = index_source(&store, &provider, "rust");
    tokio::pin!(indexing);
    tokio::select! {
        _ = &mut indexing => panic!("inference should be paused"),
        _ = provider.started.notified() => {}
    }
    ingest(
        &store,
        "rust",
        "replacement",
        &[
            (
                "proxy",
                "<main><h1>Updated proxy</h1><p>A proxy routes new requests.</p></main>",
            ),
            (
                "timeout",
                "<main><h1>Updated timeout</h1><p>Timeout limits now change.</p></main>",
            ),
            (
                "cookies",
                "<main><h1>Updated cookies</h1><p>Cookie semantics changed.</p></main>",
            ),
        ],
    )
    .await;
    provider.resume.notify_one();
    let report = indexing.await.unwrap();
    assert!(report.superseded >= 2);
    let coverage = store
        .embedding_coverage(provider.space(), "rust")
        .await
        .unwrap();
    assert!(coverage.stale >= 2);
    let clean = TestProvider::new("revision1", 3);
    index_source(&store, &clean, "rust").await.unwrap();
    assert_eq!(
        store
            .embedding_coverage(clean.space(), "rust")
            .await
            .unwrap()
            .ready,
        3
    );
    let hits = vector_search(&store, &clean, SearchQuery::new("intermediary"))
        .await
        .unwrap();
    assert!(hits.iter().all(|hit| hit.crawl_id == "replacement"));
}

/// Requires an explicitly started Ollama and installed model. The normal suite
/// never downloads weights or starts services.
#[tokio::test]
#[ignore = "requires local Ollama with ARIADNE_EMBED_MODEL installed"]
async fn real_ollama_semantic_smoke() {
    use ariadne::embeddings::{OllamaConfig, OllamaProvider};
    let (_dir, store) = fixture().await;
    let provider = OllamaProvider::connect(OllamaConfig::from_env().unwrap())
        .await
        .unwrap();
    let report = index_source(&store, &provider, "rust").await.unwrap();
    assert_eq!((report.generated, report.failed), (3, 0));
    let mut query = SearchQuery::new("How can I send HTTP traffic through an intermediary server?");
    query.source_id = Some("rust".into());
    query.limit = 1;
    assert!(search(&store, query.clone()).await.unwrap().is_empty());
    let hits = vector_search(&store, &provider, query).await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].title, "Proxy");
    assert_eq!(hits[0].url.fragment(), Some("socks"));
    println!(
        "real model: {} revision={} dimensions={} score={}",
        provider.space().model,
        provider.space().revision,
        provider.space().dimensions,
        hits[0].score
    );
    assert_eq!(
        index_source(&store, &provider, "rust")
            .await
            .unwrap()
            .reused,
        3
    );
}

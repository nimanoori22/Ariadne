use anyhow::Result;
use ariadne::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, CrawlScope, PageOutcome, PageState},
    embeddings::{
        EmbeddingProvider, EmbeddingPurpose, EmbeddingSpace, ProviderLimits, index_source,
    },
    extraction::extract,
    ingestion::{ExtractionBatch, prepare_crawl},
    retrieval::{
        ContentKind, ContextOptions, GraphOptions, MatchKind, MetadataFilter, SearchQuery,
        assemble_context, graph_hybrid_search, hybrid_search, search, vector_search,
    },
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

struct Provider {
    space: EmbeddingSpace,
    calls: Arc<AtomicUsize>,
}
impl Provider {
    fn new() -> Self {
        Self {
            space: EmbeddingSpace {
                provider: "fixture".into(),
                model: "retrieval".into(),
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
            batch_size: 4,
            max_input_bytes: 20000,
            max_batch_bytes: 80000,
        }
    }
    async fn embed(&self, input: &[String], purpose: EmbeddingPurpose) -> Result<Vec<Vec<f64>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(input
            .iter()
            .map(|s| {
                if matches!(purpose, EmbeddingPurpose::Query) {
                    if s.contains("intermediaries") {
                        vec![0., 0., 1.]
                    } else if s.contains("connection routing") {
                        vec![1., 0., 0.]
                    } else {
                        vec![0.7, 0.7, 0.]
                    }
                } else if s.contains("Proxy::custom") {
                    vec![0., 0., 1.]
                } else if s.contains("Connection routing") {
                    vec![0., 1., 0.]
                } else if s.contains("Remote dispatch") {
                    vec![1., 0., 0.]
                } else if s.contains("DEFINE INDEX") {
                    vec![0., 0., 1.]
                } else {
                    vec![0.7, 0.7, 0.]
                }
            })
            .collect())
    }
}
async fn ingest(
    store: &KnowledgeStore,
    source: &str,
    run: &str,
    pages: &[(&str, &str)],
    budget: usize,
) {
    let root = Url::parse(&format!("https://example.test/{source}/")).unwrap();
    store
        .register_source(Source::new(source, source, root.clone()).unwrap())
        .await
        .unwrap();
    let request = CrawlRequest::new(
        source,
        run,
        root.clone(),
        CrawlScope::new(root, &format!("/{source}/")).unwrap(),
    );
    store.begin_crawl(&request).await.unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(123);
    let outcomes = pages
        .iter()
        .map(|(path, html)| {
            extract(PageOutcome {
                source_id: source.into(),
                crawl_id: run.into(),
                requested_url: format!("https://example.test/{source}/{path}"),
                final_url: format!("https://example.test/{source}/{path}"),
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
                ChunkPolicy {
                    target_chars: budget,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
}
async fn fixture() -> (tempfile::TempDir, KnowledgeStore, Provider) {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    ingest(&store,"rust","initial",&[
        ("proxy","<title>Proxy</title><main><h1 id='custom'>Proxy configuration</h1><p>Proxy::custom configures SOCKS proxies.</p></main>"),
        ("routing/a","<title>Routing</title><main><h1 id='routing'>Connection routing</h1><p>Connection routing binds client connections.</p></main>"),
        ("routing/b","<title>Dispatch</title><main><h1>Remote dispatch</h1><p>Remote dispatch sends outbound requests along a chosen path.</p></main>"),
        ("routing-extra/noise","<title>Noise</title><main><h1>Cookies</h1><p>Cookie jars retain session state.</p></main>"),
    ],2400).await;
    ingest(&store,"surreal","initial",&[("indexes","<title>Indexes</title><main><h1 id='indexes'>Database schema</h1><p>DEFINE INDEX creates a database index.</p></main>")],2400).await;
    let provider = Provider::new();
    index_source(&store, &provider, "rust").await.unwrap();
    index_source(&store, &provider, "surreal").await.unwrap();
    (dir, store, provider)
}

#[tokio::test]
async fn hybrid_uses_engine_rank_fusion_and_keeps_component_evidence() {
    let (_dir, store, provider) = fixture().await;
    let ranks = store
        .fuse_ranks(
            &[vec!["a".into(), "b".into()], vec!["b".into(), "a".into()]],
            60,
        )
        .await
        .unwrap();
    assert_eq!(ranks.len(), 2);
    assert!((ranks[0].rrf_score - (1. / 61. + 1. / 62.)).abs() < 1e-12);
    let mut query = SearchQuery::new("Proxy::custom");
    query.source_id = Some("rust".into());
    query.limit = 2;
    let hits = hybrid_search(&store, &provider, query.clone())
        .await
        .unwrap();
    assert_eq!(hits[0].document_url.path(), "/rust/proxy");
    assert_eq!(hits[0].match_kind, MatchKind::Hybrid);
    let fusion = hits[0].fusion.as_ref().unwrap();
    assert_eq!(fusion.lexical_rank, Some(1));
    assert!(fusion.vector_rank.is_some());
    assert!(fusion.lexical_score.is_some());
    assert!(fusion.vector_score.is_some());
    assert_eq!(hits[0].url.fragment(), Some("custom"));
    assert_eq!(hits[0].content_kind, ContentKind::SourceData);
    assert_eq!(
        hits.iter()
            .map(|h| &h.chunk_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        hits.len()
    );
    assert_eq!(hits, hybrid_search(&store, &provider, query).await.unwrap());
}

#[tokio::test]
async fn relevance_fixture_measures_complementary_search_at_two_results() {
    let (_dir, store, provider) = fixture().await;
    let cases = [
        ("Proxy::custom", vec!["/rust/proxy"], "rust"),
        (
            "route outgoing traffic through intermediaries",
            vec!["/rust/proxy"],
            "rust",
        ),
        (
            "connection routing",
            vec!["/rust/routing/a", "/rust/routing/b"],
            "rust",
        ),
        ("DEFINE INDEX", vec!["/surreal/indexes"], "surreal"),
    ];
    let mut totals = [0.; 4];
    for (query, relevant, source) in cases.iter() {
        let mut request = SearchQuery::new(*query);
        request.limit = 2;
        request.source_id = Some((*source).into());
        let lists = [
            search(&store, request.clone()).await.unwrap(),
            vector_search(&store, &provider, request.clone())
                .await
                .unwrap(),
            hybrid_search(&store, &provider, request.clone())
                .await
                .unwrap(),
            graph_hybrid_search(&store, &provider, request, GraphOptions::default())
                .await
                .unwrap()
                .hits,
        ];
        for (i, hits) in lists.iter().enumerate() {
            totals[i] += relevant
                .iter()
                .filter(|path| hits.iter().any(|h| h.document_url.path() == **path))
                .count() as f64
                / relevant.len() as f64;
        }
    }
    let scores = totals.map(|v| v / cases.len() as f64);
    println!(
        "fixture recall@2: lexical={} vector={} hybrid={} graph={}",
        scores[0], scores[1], scores[2], scores[3]
    );
    assert!(scores[2] > scores[0] && scores[2] > scores[1], "{scores:?}");
    assert_eq!(scores[2], 1.);
    assert_eq!(scores[3], scores[2]);
}

#[tokio::test]
async fn source_and_metadata_filters_apply_before_lexical_vector_and_hybrid_limits() {
    let (_dir, store, provider) = fixture().await;
    let mut query = SearchQuery::new("connection routing");
    query.limit = 1;
    query.source_id = Some("rust".into());
    query.filter = MetadataFilter {
        url_prefix: Some("https://example.test/rust/routing".into()),
        heading: Some("CONNECTION ROUTING".into()),
        crawled_after: Some(123),
    };
    for hits in [
        search(&store, query.clone()).await.unwrap(),
        vector_search(&store, &provider, query.clone())
            .await
            .unwrap(),
        hybrid_search(&store, &provider, query.clone())
            .await
            .unwrap(),
    ] {
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document_url.path(), "/rust/routing/a");
    }
    query.filter.heading = None;
    query.limit = 2;
    let hits = vector_search(&store, &provider, query.clone())
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(
        hits.iter()
            .all(|h| h.document_url.path().starts_with("/rust/routing/"))
    );
    query.source_id = None;
    let hits = vector_search(&store, &provider, query.clone())
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(hits.iter().all(|h| h.source_id == "rust"));
    query.filter.crawled_after = Some(124);
    assert!(
        hybrid_search(&store, &provider, query)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn invalid_filters_are_rejected_before_provider_work_and_sql_literals_stay_bound() {
    let (_dir, store, provider) = fixture().await;
    let calls = provider.calls.load(Ordering::SeqCst);
    for prefix in [
        "file:///etc/passwd",
        "https://user:secret@example.test/",
        "https://example.test/?q=1",
        "not-a-url",
    ] {
        let mut query = SearchQuery::new("Proxy::custom");
        query.filter.url_prefix = Some(prefix.into());
        assert!(hybrid_search(&store, &provider, query).await.is_err());
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    let mut query = SearchQuery::new("Proxy::custom");
    query.filter.heading = Some("'; DELETE chunk; --".into());
    assert!(
        hybrid_search(&store, &provider, query)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        search(&store, SearchQuery::new("Proxy::custom"))
            .await
            .unwrap()
            .len(),
        1
    );
}

const STRUCTURED: &str = "<title>Context</title><main><h1 id='parent'>Client</h1><p>Parent explanation explains how to prepare the client safely before any network request.</p><h2 id='setup'>Setup</h2><p>Previous configuration paragraph prepares settings and options before the actual client construction.</p><p>MatchMarker configures a custom client with correct settings and checks every configuration value.</p><pre><code class='language-rust'>let client = Client::builder().build()?;</code></pre><p>Following explanation describes how to use the initialized client and handle request failures.</p><h2 id='other'>Other</h2><p>Another topic includes additional details and recommendations for advanced configuration.</p></main>";
async fn context_fixture() -> (tempfile::TempDir, KnowledgeStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    ingest(&store, "docs", "initial", &[("client", STRUCTURED)], 95).await;
    (dir, store)
}
#[tokio::test]
async fn context_expands_neighbors_preserves_code_and_parent_headings_and_deduplicates_overlap() {
    let (_dir, store) = context_fixture().await;
    let hits = search(&store, SearchQuery::new("MatchMarker"))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let context = assemble_context(&store, &hits, ContextOptions::default())
        .await
        .unwrap();
    assert_eq!(context.passages.len(), 1);
    let chunks = &context.passages[0].chunks;
    assert!(chunks.len() >= 2);
    let matched = chunks
        .iter()
        .find(|c| c.chunk_id == hits[0].chunk_id)
        .unwrap();
    assert_eq!(matched.heading_path, ["Client", "Setup"]);
    assert!(
        matched
            .text
            .contains("let client = Client::builder().build()?;")
    );
    assert_eq!(matched.url.fragment(), Some("setup"));
    assert_eq!(matched.content_kind, ContentKind::SourceData);
    assert!(chunks.iter().all(|c| !c.text_truncated));
    let mut combined = hits.clone();
    combined.extend(
        search(&store, SearchQuery::new("Following explanation"))
            .await
            .unwrap(),
    );
    let context = assemble_context(&store, &combined, ContextOptions::default())
        .await
        .unwrap();
    assert!(context.deduplicated_chunks > 0);
    let chunks = &context.passages[0].chunks;
    assert_eq!(
        chunks
            .iter()
            .map(|c| &c.chunk_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        chunks.len()
    );
    assert_eq!(context.passages[0].matches.len(), 2);
}
#[tokio::test]
async fn context_budgets_are_global_unicode_safe_and_stale_hits_are_explicit() {
    let (_dir, store) = context_fixture().await;
    let hits = search(&store, SearchQuery::new("MatchMarker"))
        .await
        .unwrap();
    let context = assemble_context(
        &store,
        &hits,
        ContextOptions {
            max_total_chars: 19,
            max_chunk_chars: 100,
            max_chunks: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(context.total_text_chars, 19);
    assert!(context.budget_exhausted);
    assert!(context.passages[0].chunks[0].text_truncated);
    assert_eq!(context.passages[0].chunks[0].chunk_id, hits[0].chunk_id);
    ingest(&store,"docs","changed",&[("client","<main><h1>Changed</h1><p>Replacement content removes the matched section entirely.</p></main>")],95).await;
    let context = assemble_context(&store, &hits, ContextOptions::default())
        .await
        .unwrap();
    assert_eq!(context.skipped_stale_hits, 1);
    assert!(context.passages.is_empty());
    assert!(
        assemble_context(
            &store,
            &[],
            ContextOptions {
                neighbor_chunks: 4,
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn cli_lexical_context_and_metadata_filters_work_without_ollama() {
    let (dir, store) = context_fixture().await;
    drop(store);
    let path = dir.path().to_owned();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_Ariadne"))
            .env("ARIADNE_DATA_DIR", path)
            .args([
                "retrieve",
                "MatchMarker",
                "--retrieval-mode",
                "lexical",
                "--source",
                "docs",
                "--heading",
                "Setup",
                "--context-chars",
                "1000",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["passages"].as_array().unwrap().len(), 1);
    assert!(json["total_text_chars"].as_u64().unwrap() <= 1000);
}

#[tokio::test]
async fn context_budget_spans_documents_and_slices_multibyte_text_without_losing_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    let html = "<title>Unicode</title><main><h1 id='unicode'>Unicode</h1><p>UnicodeMarker فارسی 😀 é 日本語 repeated explanation continues with more text for budgeting.</p></main>";
    ingest(&store, "one", "initial", &[("page", html)], 2400).await;
    ingest(&store, "two", "initial", &[("page", html)], 2400).await;
    let hits = search(&store, SearchQuery::new("UnicodeMarker"))
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    let context = assemble_context(
        &store,
        &hits,
        ContextOptions {
            neighbor_chunks: 0,
            max_chunk_chars: 35,
            max_total_chars: 60,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(context.passages.len(), 2);
    assert_eq!(context.total_text_chars, 60);
    assert!(context.budget_exhausted);
    assert_eq!(
        context
            .passages
            .iter()
            .flat_map(|p| &p.chunks)
            .map(|c| c.text.chars().count())
            .sum::<usize>(),
        60
    );
    assert!(
        context
            .passages
            .iter()
            .flat_map(|p| &p.chunks)
            .all(|c| c.text_truncated
                && c.url.fragment() == Some("unicode")
                && c.crawl_id == "initial")
    );
    assert!(context.passages[0].chunks[0].text.contains("فارسی"));
}

#[tokio::test]
async fn ranking_strategy_is_replaceable_and_invalid_candidate_output_is_rejected() {
    use ariadne::retrieval::{HybridRanker, RankedChunk, hybrid_search_with_ranker};
    struct Reverse;
    impl HybridRanker for Reverse {
        fn name(&self) -> &str {
            "fixture_reverse"
        }
        async fn rank(
            &self,
            _store: &KnowledgeStore,
            lists: &[Vec<String>],
        ) -> Result<Vec<RankedChunk>> {
            Ok(lists[1]
                .iter()
                .rev()
                .enumerate()
                .map(|(index, id)| RankedChunk {
                    id: id.clone(),
                    rrf_score: (lists[1].len() - index) as f64,
                })
                .collect())
        }
    }
    struct Invalid;
    impl HybridRanker for Invalid {
        fn name(&self) -> &str {
            "invalid"
        }
        async fn rank(
            &self,
            _store: &KnowledgeStore,
            _lists: &[Vec<String>],
        ) -> Result<Vec<RankedChunk>> {
            Ok(vec![RankedChunk {
                id: "not-a-candidate".into(),
                rrf_score: 1.,
            }])
        }
    }
    let (_dir, store, provider) = fixture().await;
    let mut query = SearchQuery::new("Proxy::custom");
    query.source_id = Some("rust".into());
    let ranked = hybrid_search_with_ranker(&store, &provider, query.clone(), &Reverse)
        .await
        .unwrap();
    assert_eq!(
        ranked[0].fusion.as_ref().unwrap().strategy,
        "fixture_reverse"
    );
    assert!(
        hybrid_search_with_ranker(&store, &provider, query, &Invalid)
            .await
            .is_err()
    );
}

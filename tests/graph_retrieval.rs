use anyhow::Result;
use ariadne::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, CrawlScope, PageOutcome, PageState},
    embeddings::{
        EmbeddingProvider, EmbeddingPurpose, EmbeddingSpace, OllamaConfig, ProviderLimits,
        index_source,
    },
    extraction::extract,
    ingestion::{ExtractionBatch, prepare_crawl},
    mcp::KnowledgeMcp,
    retrieval::{
        ContextOptions, GraphOptions, GraphRelation, MetadataFilter, SearchQuery, assemble_context,
        graph_hybrid_search, graph_search, graph_vector_search, hybrid_search, search,
        vector_search,
    },
    storage::{KnowledgeStore, Source},
};
use rmcp::{ServiceExt, model::CallToolRequestParams};
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, UNIX_EPOCH},
};
use url::Url;

struct Provider {
    space: EmbeddingSpace,
    calls: AtomicUsize,
}
impl Provider {
    fn new() -> Self {
        Self {
            space: EmbeddingSpace {
                provider: "fixture".into(),
                model: "graph".into(),
                revision: "v1".into(),
                dimensions: 3,
                input_version: "v1".into(),
            },
            calls: AtomicUsize::new(0),
        }
    }
}
impl EmbeddingProvider for Provider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }
    fn limits(&self) -> ProviderLimits {
        ProviderLimits {
            batch_size: 8,
            max_input_bytes: 20000,
            max_batch_bytes: 160000,
        }
    }
    async fn embed(&self, inputs: &[String], purpose: EmbeddingPurpose) -> Result<Vec<Vec<f64>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(inputs
            .iter()
            .map(|s| {
                if matches!(purpose, EmbeddingPurpose::Query) || s.contains("Marker") {
                    vec![1., 0., 0.]
                } else if s.contains("Noise") {
                    vec![0.9, 0.1, 0.]
                } else {
                    vec![0., 0., 1.]
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
    store
        .begin_crawl(&CrawlRequest::new(
            source,
            run,
            root.clone(),
            CrawlScope::new(root, &format!("/{source}")).unwrap(),
        ))
        .await
        .unwrap();
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
const NOISE: &str = "<main><h1>Noise</h1><p>Noise about unrelated settings and cookies.</p></main>";
const LINK_SEED: &str = "<main><h1>Gateway</h1><p>GatewayMarker configures outbound traffic.</p><a href='target#deep%20setup'>Read setup</a><a href='missing'>Missing</a><a href='https://elsewhere.test/escape'>Escape</a></main>";
const LINK_TARGET: &str = "<main><h1 id='intro'>Introduction</h1><p>Unrelated overview.</p><h2 id='deep setup'>Essential settings</h2><p>Connect using the supported TLS settings and timeout options.</p></main>";
const ENTITY_SEED: &str =
    "<main><h1>Routing</h1><p>EntityMarker uses <code>Client::route</code>.</p></main>";
const ENTITY_TARGET: &str = "<main><h1 id='details'>Timeout handling</h1><p>Configure timeouts.</p><pre><code>Client::route();</code></pre></main>";
async fn fixture() -> (tempfile::TempDir, KnowledgeStore, Provider) {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path().join("knowledge"))
        .await
        .unwrap();
    for (source, seed, target) in [
        ("links", LINK_SEED, LINK_TARGET),
        ("entities", ENTITY_SEED, ENTITY_TARGET),
        (
            "incoming",
            "<main><h1>Guide</h1><p>More explanation about supported settings.</p><a href='target#def'>Definition</a></main>",
            "<main><h1 id='def'>Definition</h1><p>InboundMarker specifies the interface.</p></main>",
        ),
    ] {
        ingest(
            &store,
            source,
            "initial",
            &[
                ("start", seed),
                ("target", target),
                ("noise0", NOISE),
                ("noise1", NOISE),
                ("noise2", NOISE),
            ],
            2400,
        )
        .await;
    }
    let provider = Provider::new();
    for source in ["links", "entities", "incoming"] {
        index_source(&store, &provider, source).await.unwrap();
    }
    (dir, store, provider)
}
fn query(text: &str, source: &str) -> SearchQuery {
    let mut q = SearchQuery::new(text);
    q.source_id = Some(source.into());
    q.limit = 2;
    q
}
fn options() -> GraphOptions {
    GraphOptions {
        max_seeds: 1,
        ..Default::default()
    }
}

#[tokio::test]
async fn graph_fixture_measures_link_and_entity_recall_against_individual_and_hybrid_modes() {
    let (_dir, store, provider) = fixture().await;
    let cases = [
        ("GatewayMarker", "links", ["/links/start", "/links/target"]),
        (
            "EntityMarker",
            "entities",
            ["/entities/start", "/entities/target"],
        ),
        (
            "InboundMarker",
            "incoming",
            ["/incoming/target", "/incoming/start"],
        ),
    ];
    let mut totals = [0.; 4];
    for (text, source, relevant) in cases {
        let request = query(text, source);
        let enhanced = graph_hybrid_search(&store, &provider, request.clone(), options())
            .await
            .unwrap();
        assert!(enhanced.graph.candidates > 0, "{enhanced:?}");
        let lists = [
            search(&store, request.clone()).await.unwrap(),
            vector_search(&store, &provider, request.clone())
                .await
                .unwrap(),
            hybrid_search(&store, &provider, request).await.unwrap(),
            enhanced.hits,
        ];
        for (i, hits) in lists.iter().enumerate() {
            totals[i] += relevant
                .iter()
                .filter(|p| hits.iter().any(|h| h.document_url.path() == **p))
                .count() as f64
                / 2.;
        }
        assert!(lists[3].iter().all(|h| h.source_id == source));
        let related = lists[3].iter().find(|h| h.graph.is_some()).unwrap();
        let path = &related.graph.as_ref().unwrap().paths[0];
        assert_eq!(
            path.relation,
            match source {
                "links" => GraphRelation::OutgoingLink,
                "entities" => GraphRelation::SharedEntity,
                _ => GraphRelation::IncomingLink,
            }
        );
        assert!(related.fusion.as_ref().unwrap().graph_rank.is_some());
        if source == "links" {
            assert_eq!(related.url.fragment(), Some("deep%20setup"));
            assert!(related.text.contains("TLS"));
            assert!(!related.text.contains("Unrelated overview"));
        }
        if source == "entities" {
            assert_eq!(path.entity.as_deref(), Some("Client::route"));
        }
    }
    let scores = totals.map(|v| v / 3.);
    println!(
        "relation fixture recall@2: lexical={} vector={} hybrid={} graph={}",
        scores[0], scores[1], scores[2], scores[3]
    );
    assert_eq!(scores, [0.5, 0.5, 0.5, 1.]);
}

#[tokio::test]
async fn graph_candidates_obey_filters_budgets_source_boundaries_and_context_provenance() {
    let (_dir, store, provider) = fixture().await;
    let request = query("GatewayMarker", "links");
    let response = graph_search(&store, request.clone(), options())
        .await
        .unwrap();
    assert_eq!(response.graph.candidates, 1);
    assert_eq!(
        response,
        graph_search(&store, request.clone(), options())
            .await
            .unwrap()
    );
    let target = response
        .hits
        .iter()
        .find(|h| h.document_url.path() == "/links/target")
        .unwrap();
    let context = assemble_context(
        &store,
        &response.hits,
        ContextOptions {
            neighbor_chunks: 0,
            max_total_chars: 200,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(context.total_text_chars <= 200);
    let evidence = context
        .passages
        .iter()
        .flat_map(|p| &p.matches)
        .find(|m| m.chunk_id == target.chunk_id)
        .unwrap();
    assert_eq!(evidence.graph, target.graph);
    let mut filtered = request.clone();
    filtered.filter = MetadataFilter {
        heading: Some("Gateway".into()),
        ..Default::default()
    };
    let response = graph_search(&store, filtered, options()).await.unwrap();
    assert_eq!(response.graph.candidates, 0);
    assert_eq!(response.hits.len(), 1);
    assert_eq!(
        response.hits[0].match_kind,
        ariadne::retrieval::MatchKind::FullText
    );
    let mut filtered = request.clone();
    filtered.filter.url_prefix = Some("https://example.test/links/start".into());
    assert_eq!(
        graph_search(&store, filtered, options())
            .await
            .unwrap()
            .graph
            .candidates,
        0
    );
    let mut filtered = request.clone();
    filtered.filter.crawled_after = Some(124);
    assert!(
        graph_hybrid_search(&store, &provider, filtered, options())
            .await
            .unwrap()
            .hits
            .is_empty()
    );
    let mut all = request.clone();
    all.source_id = None;
    let response = graph_search(&store, all, options()).await.unwrap();
    assert!(response.hits.iter().all(|h| h.source_id == "links"));
    let entities = graph_search(&store, query("EntityMarker", "entities"), options())
        .await
        .unwrap();
    assert_eq!(entities.graph.candidates, 1);
    assert!(
        graph_search(
            &store,
            query("EntityMarker", "entities"),
            GraphOptions {
                entities: false,
                ..options()
            }
        )
        .await
        .unwrap()
        .graph
        .candidates
            == 0
    );
    assert!(
        graph_search(
            &store,
            query("GatewayMarker", "links"),
            GraphOptions {
                links: false,
                ..options()
            }
        )
        .await
        .unwrap()
        .graph
        .candidates
            == 0
    );
    let vector = graph_vector_search(&store, &provider, request, options())
        .await
        .unwrap();
    assert!(vector.hits.iter().any(|h| h.graph.is_some()));
    let calls = provider.calls.load(Ordering::SeqCst);
    assert!(
        graph_hybrid_search(
            &store,
            &provider,
            query("GatewayMarker", "links"),
            GraphOptions {
                max_seeds: 9,
                ..options()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
}

fn call(name: &str, args: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_owned()).with_arguments(args.as_object().unwrap().clone())
}
#[tokio::test]
async fn graph_assistance_works_through_real_mcp_and_cli_without_ollama() {
    let (dir, store, _provider) = fixture().await;
    let (a, b) = tokio::io::duplex(65536);
    let server = tokio::spawn(
        KnowledgeMcp::new(
            store,
            OllamaConfig {
                endpoint: Url::parse("http://127.0.0.1:1/").unwrap(),
                model: "unavailable".into(),
            },
        )
        .serve(a),
    );
    let client = ().serve(b).await.unwrap();
    let server = server.await.unwrap().unwrap();
    let result=client.call_tool(call("search",json!({"query":"GatewayMarker","source_id":"links","mode":"lexical","limit":2,"graph":{"max_seeds":1},"context":{"neighbor_chunks":0,"max_total_chars":500}}))).await.unwrap();
    assert_ne!(result.is_error, Some(true), "{result:?}");
    let result = result.structured_content.unwrap();
    assert_eq!(result["graph"]["candidates"], 1);
    assert_eq!(result["hits"].as_array().unwrap().len(), 2);
    assert!(
        result["context"]["passages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["matches"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m.get("graph").is_some()))
    );
    for graph in [
        json!({"max_seeds":9}),
        json!({"max_edges_per_seed":0}),
        json!({"max_chunks_per_edge":4}),
        json!({"max_candidates":51}),
        json!({"links":false,"entities":false}),
        json!({"unknown":true}),
    ] {
        assert!(
            client
                .call_tool(call(
                    "search",
                    json!({"query":"GatewayMarker","mode":"vector","graph":graph})
                ))
                .await
                .is_err()
        );
    }
    let ordinary = client
        .call_tool(call(
            "search",
            json!({"query":"GatewayMarker","source_id":"links"}),
        ))
        .await
        .unwrap()
        .structured_content
        .unwrap();
    assert!(ordinary.get("graph").is_none());
    assert_eq!(ordinary["hits"].as_array().unwrap().len(), 1);
    client.cancel().await.unwrap();
    server.cancel().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let cli = |args: &[&str]| {
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
    let hits = cli(&[
        "graph-search",
        "GatewayMarker",
        "--retrieval-mode",
        "lexical",
        "--source",
        "links",
        "--limit",
        "2",
        "--graph-seeds",
        "1",
    ]);
    assert_eq!(hits["graph"]["candidates"], 1);
    assert_eq!(hits["hits"].as_array().unwrap().len(), 2);
    let context = cli(&[
        "retrieve",
        "GatewayMarker",
        "--retrieval-mode",
        "lexical",
        "--source",
        "links",
        "--graph",
        "--graph-seeds",
        "1",
        "--context-chars",
        "500",
    ]);
    assert_eq!(context["graph"]["candidates"], 1);
    assert!(context["total_text_chars"].as_u64().unwrap() <= 500);
}

#[tokio::test]
async fn hubs_and_repeated_paths_report_truncation_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path()).await.unwrap();
    let hub = format!(
        "<main><h1>Hub</h1><p>HubMarker</p><p>{}</p></main>",
        (0..5)
            .map(|n| format!("<a href='target{n}#root'>Next</a>"))
            .collect::<String>()
    );
    let target = "<main><h1 id='root'>Target</h1><p>First long explanation about network settings goes here for the documentation.</p><p>Second long explanation about storage settings goes here for the documentation.</p><p>Third long explanation describes startup behavior and resource limits in detail.</p></main>";
    ingest(
        &store,
        "hub",
        "initial",
        &[
            ("start", &hub),
            ("target0", target),
            ("target1", target),
            ("target2", target),
            ("target3", target),
            ("target4", target),
        ],
        80,
    )
    .await;
    let mut request = query("HubMarker", "hub");
    request.max_text_chars = 9;
    let bounded = GraphOptions {
        max_seeds: 1,
        max_edges_per_seed: 2,
        max_chunks_per_edge: 1,
        max_candidates: 1,
        ..Default::default()
    };
    let response = graph_search(&store, request, bounded).await.unwrap();
    assert_eq!(response.graph.candidates, 1);
    assert!(response.graph.candidates_truncated);
    assert!(response.graph.traversal_truncated);
    assert_eq!(response.hits.len(), 2);
    assert!(
        response
            .hits
            .iter()
            .all(|h| h.text.chars().count() <= 9 && h.text_truncated)
    );
    let shared = "<main><h1>Sharing</h1><p>SharingMarker <code>Shared::one Shared::two Shared::three Shared::four Shared::five Shared::six</code></p></main>";
    let target = shared.replace("SharingMarker", "Related explanation");
    ingest(
        &store,
        "dup",
        "initial",
        &[("start", shared), ("target", &target)],
        2400,
    )
    .await;
    let response = graph_search(
        &store,
        query("SharingMarker", "dup"),
        GraphOptions {
            max_seeds: 1,
            max_edges_per_seed: 8,
            links: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.graph.candidates, 1);
    let evidence = response.hits.iter().find_map(|h| h.graph.as_ref()).unwrap();
    assert_eq!(evidence.paths.len(), 4);
    assert!(evidence.paths_truncated);
    assert_eq!(
        response
            .hits
            .iter()
            .map(|h| &h.chunk_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        response.hits.len()
    );
}

#[tokio::test]
async fn shared_entity_filters_apply_before_fanout_limits_and_never_cross_sources() {
    let dir = tempfile::tempdir().unwrap();
    let store = KnowledgeStore::open(dir.path()).await.unwrap();
    let seed =
        "<main><h1>Allowed</h1><p>FilteredMarker uses <code>Client::route</code>.</p></main>";
    let disallowed = "<main><h1>Disallowed</h1><p><code>Client::route</code> appears in irrelevant notes.</p></main>";
    let allowed = "<main><h1>Allowed</h1><p><code>Client::route</code> appears in the useful instructions.</p></main>";
    ingest(
        &store,
        "filtered",
        "initial",
        &[
            ("seed", seed),
            ("a0", disallowed),
            ("a1", disallowed),
            ("a2", disallowed),
            ("a3", disallowed),
            ("a4", disallowed),
            ("z", allowed),
        ],
        2400,
    )
    .await;
    ingest(&store, "shadow", "initial", &[("z", allowed)], 2400).await;
    let mut request = query("FilteredMarker", "filtered");
    request.filter.heading = Some("Allowed".into());
    request.source_id = None;
    let response = graph_search(
        &store,
        request,
        GraphOptions {
            max_seeds: 1,
            max_chunks_per_edge: 1,
            links: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.graph.candidates, 1);
    assert!(response.hits.iter().all(|h| h.source_id == "filtered"));
    assert!(
        response
            .hits
            .iter()
            .any(|h| h.document_url.path() == "/filtered/z")
    );
    assert!(!response.graph.traversal_truncated);
}

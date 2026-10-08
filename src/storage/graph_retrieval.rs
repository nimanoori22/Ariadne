//! Snapshot and bound relation candidates before rank fusion.
use super::*;
use crate::retrieval::{GraphOptions, KnowledgeHit, SearchQuery, graph::GraphSeedRow};

impl KnowledgeStore {
    pub(crate) async fn graph_neighbors(
        &self,
        seeds: &[KnowledgeHit],
        query: &SearchQuery,
        options: GraphOptions,
    ) -> Result<Vec<GraphSeedRow>> {
        query.validate()?;
        options.validate()?;
        ensure!(
            seeds.len() <= options.max_seeds,
            "graph seed limit exceeded"
        );
        if seeds.is_empty() {
            return Ok(vec![]);
        }
        let projection = include_str!("search_projection.surql")
            .replace("search::score(0) AS score", "0 AS score")
            .replace("'full_text' AS match_kind", "'graph' AS match_kind");
        let candidates = |sql: &str| {
            sql.replace("-- HIT_PROJECTION", &projection)
                .replace(
                    "-- RELATION_METADATA_FILTER",
                    &include_str!("metadata_filter.surql").replace("data.", "in.data."),
                )
                .replace("-- METADATA_FILTER", include_str!("metadata_filter.surql"))
        };
        let sql = include_str!("graph_neighbors.surql")
            .replace(
                "-- LINK_CANDIDATES",
                &candidates(include_str!("graph_link_candidates.surql")),
            )
            .replace(
                "-- ENTITY_CANDIDATES",
                &candidates(include_str!("graph_entity_candidates.surql")),
            );
        let matches: Vec<Value> = seeds.iter().map(|h| json!({"id":h.chunk_id,"hash":h.content_sha256,"source":h.source_id,"url":h.document_url,"source_url":h.url})).collect();
        let mut result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.db
                .query(sql)
                .bind(("seeds", matches))
                .bind(("version", crate::graph::GRAPH_VERSION))
                .bind(("max_edges", options.max_edges_per_seed))
                .bind(("edge_limit", options.max_edges_per_seed + 1))
                .bind(("chunk_limit", options.max_chunks_per_edge + 1))
                .bind(("max_text_chars", query.max_text_chars))
                .bind(("source_id", query.source_id.clone().unwrap_or_default()))
                .bind(("links", options.links))
                .bind(("entities", options.entities))
                .bind(filter_bindings(&query.filter)?),
        )
        .await
        .context("graph retrieval timed out")??
        .check()?;
        let rows: Vec<Value> = result.take(1)?;
        rows.into_iter()
            .map(|row| serde_json::from_value(row).map_err(Into::into))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crawler::{PageOutcome, PageState},
        extraction::extract,
        ingestion::ExtractionBatch,
        retrieval::{graph::expand_candidates, graph_search, search},
    };

    async fn setup() -> (
        tempfile::TempDir,
        KnowledgeStore,
        SearchQuery,
        Vec<KnowledgeHit>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = KnowledgeStore::open(dir.path()).await.unwrap();
        let root = Url::parse("https://example.test/docs/").unwrap();
        store
            .register_source(Source::new("docs", "Docs", root.clone()).unwrap())
            .await
            .unwrap();
        let request = CrawlRequest::new(
            "docs",
            "initial",
            root.clone(),
            CrawlScope::new(root.clone(), "/docs").unwrap(),
        );
        store.begin_crawl(&request).await.unwrap();
        let now = SystemTime::now();
        let outcomes=[("seed","<main><h1>Entry</h1><p>SeedMarker <code>Client::new</code></p><a href='target#detail'>Details</a></main>"),("target","<main><h1 id='detail'>Details</h1><p><code>Client::new</code> supports this operation.</p></main>")].into_iter().map(|(path,html)|extract(PageOutcome {source_id:"docs".into(),crawl_id:"initial".into(),requested_url:root.join(path).unwrap().to_string(),final_url:root.join(path).unwrap().to_string(),fetched_at:now,status:200,headers:vec![],raw_body:html.as_bytes().to_vec(),content_truncated:false,rendering:None,state:PageState::Fetched})).collect();
        store
            .finish_crawl(ExtractionBatch {
                source_id: "docs".into(),
                crawl_id: "initial".into(),
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
        let mut query = SearchQuery::new("SeedMarker");
        query.source_id = Some("docs".into());
        let hits = search(&store, query.clone()).await.unwrap();
        (dir, store, query, hits)
    }
    #[tokio::test]
    async fn graph_snapshot_rejects_stale_removed_and_incompatible_evidence() {
        let (_dir, store, query, seeds) = setup().await;
        let options = GraphOptions {
            max_seeds: 1,
            ..Default::default()
        };
        let (hits, report, _) = expand_candidates(&store, &seeds, &query, options)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(report.candidates, 1);
        store.db.query("UPDATE document SET data.graph.truncated=true WHERE canonical_url='https://example.test/docs/seed';").await.unwrap().check().unwrap();
        assert_eq!(
            graph_search(&store, query.clone(), options)
                .await
                .unwrap()
                .graph
                .truncated_seed_indexes,
            1
        );
        store.db.query("UPDATE document SET data.graph.version='old' WHERE canonical_url='https://example.test/docs/seed';").await.unwrap().check().unwrap();
        let (hits, report, _) = expand_candidates(&store, &seeds, &query, options)
            .await
            .unwrap();
        assert!(hits.is_empty());
        assert_eq!(report.unindexed_seeds, 1);
        store.db.query("UPDATE document SET data.graph.version=$version WHERE canonical_url='https://example.test/docs/seed'; UPDATE document SET data.revalidation.availability='removed' WHERE canonical_url='https://example.test/docs/target';").bind(("version",crate::graph::GRAPH_VERSION)).await.unwrap().check().unwrap();
        assert!(
            graph_search(&store, query.clone(), options)
                .await
                .unwrap()
                .graph
                .candidates
                == 0
        );
        store.db.query("UPDATE document SET data.revalidation.availability='active'; UPDATE mentions SET data.content_sha256='obsolete'; DELETE links_to;").await.unwrap().check().unwrap();
        assert_eq!(
            graph_search(&store, query.clone(), options)
                .await
                .unwrap()
                .graph
                .candidates,
            0
        );
        store
            .db
            .query("DELETE chunk WHERE data.id=$id;")
            .bind(("id", seeds[0].chunk_id.clone()))
            .await
            .unwrap()
            .check()
            .unwrap();
        let (hits, report, stale) = expand_candidates(&store, &seeds, &query, options)
            .await
            .unwrap();
        assert!(hits.is_empty());
        assert_eq!(report.skipped_stale_seeds, 1);
        assert!(stale.contains(&seeds[0].chunk_id));
    }
}

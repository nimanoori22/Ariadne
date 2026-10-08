//! Bounded one-hop knowledge graph reads. SurrealQL stays in this boundary.
use super::*;

fn validate(source: &str, limit: usize) -> Result<()> {
    ensure!(
        !source.trim().is_empty() && source.len() <= 1024,
        "invalid source ID"
    );
    ensure!((1..=100).contains(&limit), "graph limit must be 1..100");
    Ok(())
}

impl KnowledgeStore {
    /// Canonical document identities only. Unindexed targets are returned as
    /// such; this operation never fetches or guesses redirect aliases.
    pub async fn document_links(
        &self,
        source: &str,
        url: &Url,
        incoming: bool,
        limit: usize,
    ) -> Result<Value> {
        validate(source, limit)?;
        ensure!(url.as_str().len() <= 4096, "document URL too long");
        crate::crawler::CrawlScope::new(url.clone(), url.path())?;
        let mut url = url.clone();
        url.set_fragment(None);
        let direction = if incoming { "out" } else { "in" };
        let sql = format!("BEGIN TRANSACTION;
            LET $document = type::record('document', [$source, $url]);
            SELECT VALUE {{graph: data.graph, availability: data.revalidation.availability ?? 'active'}} FROM $document;
            SELECT VALUE object::extend(data, {{content_kind:'source_data',
                target_indexed: record::exists(out), target_availability: IF record::exists(out) {{ out.data.revalidation.availability ?? 'active' }} ELSE {{ 'unindexed' }}}})
            FROM links_to WHERE {direction}=$document AND (in.data.revalidation.availability ?? 'active') != 'removed'
                AND record::exists($document) AND ($document.data.revalidation.availability ?? 'active') != 'removed'
            ORDER BY data.document_url, data.url LIMIT $limit;
            {} COMMIT TRANSACTION;", include_str!("graph_coverage.surql"));
        let mut response = self
            .db
            .query(sql)
            .bind(("source", source.to_owned()))
            .bind(("url", url.to_string()))
            .bind(("limit", limit + 1))
            .bind(("version", crate::graph::GRAPH_VERSION))
            .await?
            .check()?;
        let documents: Vec<Value> = response.take(2)?;
        let mut links: Vec<Value> = response.take(3)?;
        let coverage: Vec<Value> = response.take(4)?;
        let truncated = links.len() > limit;
        links.truncate(limit);
        Ok(
            json!({"content_kind":"source_data", "source_id":source, "document_url":url,
            "direction":if incoming {"incoming"} else {"outgoing"}, "document":documents.first(),
            "links":links, "truncated":truncated, "coverage":coverage.first()}),
        )
    }

    /// Exact entity lookup, scoped before the limit. Mentions are evidence of
    /// spelling in code/qualified headings, not a claim about symbol resolution.
    pub async fn entity_mentions(&self, source: &str, symbol: &str, limit: usize) -> Result<Value> {
        validate(source, limit)?;
        ensure!(
            crate::graph::valid_entity(symbol),
            "entity must be an exact qualified Rust-style path"
        );
        let mut response = self.db.query(format!("BEGIN TRANSACTION;
            SELECT VALUE object::extend(data, {{content_kind:'source_data'}}) FROM mentions
                WHERE out=type::record('entity',['rust_path',$symbol]) AND data.source_id=$source
                AND record::exists(in) AND (document.data.revalidation.availability ?? 'active') != 'removed'
                ORDER BY data.document_url, data.section_id, data.chunk_id LIMIT $limit;
            {} COMMIT TRANSACTION;", include_str!("graph_coverage.surql")))
            .bind(("source",source.to_owned())).bind(("symbol",symbol.to_owned())).bind(("limit",limit+1))
            .bind(("version",crate::graph::GRAPH_VERSION)).await?.check()?;
        let mut mentions: Vec<Value> = response.take(1)?;
        let coverage: Vec<Value> = response.take(2)?;
        let truncated = mentions.len() > limit;
        mentions.truncate(limit);
        Ok(
            json!({"content_kind":"source_data","source_id":source,"entity":{"symbol":symbol,"kind":"rust_path"},
            "mentions":mentions,"truncated":truncated,"graph_version":crate::graph::GRAPH_VERSION,
            "coverage":coverage.first()}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crawler::{CrawlReport, PageOutcome, PageState},
        extraction::extract,
        ingestion::prepare_incremental,
    };

    #[tokio::test]
    async fn legacy_graph_gap_is_reported_and_backfilled_from_retained_304_content() {
        let dir = tempfile::tempdir().unwrap();
        let store = KnowledgeStore::open(dir.path()).await.unwrap();
        let url = Url::parse("https://example.test/docs/client").unwrap();
        store
            .register_source(Source::new("docs", "Docs", url.clone()).unwrap())
            .await
            .unwrap();
        let request = |run| {
            CrawlRequest::new(
                "docs",
                run,
                url.clone(),
                CrawlScope::new(url.clone(), "/docs").unwrap(),
            )
        };
        let now = SystemTime::now();
        let mut page = PageOutcome {
            source_id: "docs".into(), crawl_id: "old".into(), requested_url: url.to_string(), final_url: url.to_string(), fetched_at: now,
            status: 200, headers: vec![], raw_body: b"<main><h1>API</h1><p><code>Client::new</code></p><a href='proxy#setup'>Proxy</a></main>".to_vec(),
            content_truncated: false, rendering: None, state: PageState::Fetched,
        };
        store.begin_crawl(&request("old")).await.unwrap();
        store
            .finish_crawl(ExtractionBatch {
                source_id: "docs".into(),
                crawl_id: "old".into(),
                started_at: now,
                finished_at: now,
                outcomes: vec![extract(page.clone())],
                blocked: vec![],
                dropped_pages: 0,
                audit_overflow: false,
                discovery: Default::default(),
            })
            .await
            .unwrap();
        let original_chunks = store.get_chunks("docs", &url).await.unwrap();
        // Simulate records from before graph indexing. The additive schema must
        // not pretend they are indexed just because the new tables exist.
        store.db.query("DELETE links_to; DELETE mentions; DELETE entity; UPDATE document SET data.graph=NONE;").await.unwrap().check().unwrap();
        let missing = store
            .entity_mentions("docs", "Client::new", 20)
            .await
            .unwrap();
        assert_eq!(missing["coverage"]["documents"], 1);
        assert_eq!(missing["coverage"]["indexed_documents"], 0);
        assert!(missing["mentions"].as_array().unwrap().is_empty());
        store.begin_crawl(&request("upgrade")).await.unwrap();
        page.crawl_id = "upgrade".into();
        page.status = 304;
        page.state = PageState::NotModified;
        page.raw_body.clear();
        let prepared = prepare_incremental(
            &store,
            CrawlReport {
                source_id: "docs".into(),
                crawl_id: "upgrade".into(),
                started_at: now,
                finished_at: now,
                pages: vec![page],
                blocked: vec![],
                dropped_pages: 0,
                audit_overflow: false,
                discovery: Default::default(),
            },
            Default::default(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(prepared.recrawl.as_ref().unwrap().reprocessed, 1);
        store.finish_prepared_crawl(prepared).await.unwrap();
        let mentions = store
            .entity_mentions("docs", "Client::new", 20)
            .await
            .unwrap();
        assert_eq!(mentions["coverage"]["indexed_documents"], 1);
        assert_eq!(mentions["mentions"].as_array().unwrap().len(), 1);
        assert_eq!(mentions["mentions"][0]["chunk_id"], original_chunks[0].id);
        assert_eq!(
            store.document_links("docs", &url, false, 20).await.unwrap()["links"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store.get_chunks("docs", &url).await.unwrap()[0].id,
            original_chunks[0].id
        );
    }
}

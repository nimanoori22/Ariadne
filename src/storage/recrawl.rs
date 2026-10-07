use super::*;
use crate::{
    chunking::IndexMetadata,
    crawler::{PageCache, PageOutcome, PageState},
};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::sync::Arc;

struct StoredPageCache {
    db: Surreal<Any>,
    source: String,
    urls: Vec<String>,
}

#[async_trait]
impl PageCache for StoredPageCache {
    fn urls(&self) -> &[String] {
        &self.urls
    }
    async fn get(&self, url: &str) -> Result<Option<PageOutcome>> {
        let mut result = self.db.query("SELECT data.page AS page, data.indexing AS indexing, data.revalidation AS revalidation FROM type::record('document', [$source, $url]);")
            .bind(("source", self.source.clone())).bind(("url", url.to_owned())).await?.check()?;
        let mut rows: Vec<Value> = result.take(0)?;
        let Some(row) = rows.pop() else {
            return Ok(None);
        };
        if row["revalidation"]["availability"] == "removed" {
            return Ok(None);
        }
        let mut page: PageOutcome = serde_json::from_value(row["page"].clone())?;
        if page.state != PageState::Fetched || page.status != 200 {
            return Ok(None);
        }
        // A conditional validator is useful only with an intact retained body.
        if let Some(hash) = row["indexing"]["hashes"]["raw_sha256"].as_str()
            && format!("{:x}", Sha256::digest(&page.raw_body)) != hash
        {
            return Ok(None);
        }
        if let Some(headers) = row["revalidation"]["headers"].as_array() {
            page.headers = serde_json::from_value(Value::Array(headers.clone()))?;
        }
        Ok(Some(page))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct RecrawlSnapshot {
    pub indexing: Option<IndexMetadata>,
    #[serde(default)]
    pub revalidation: Option<Value>,
}

impl KnowledgeStore {
    pub(crate) async fn document_count(
        &self,
        source: &str,
        include_removed: bool,
    ) -> Result<usize> {
        let mut result = self.db.query("SELECT count() AS count FROM document WHERE source = type::record('source', $source) AND ($include_removed OR (data.revalidation.availability ?? 'active') != 'removed') GROUP ALL;")
            .bind(("source", source.to_owned())).bind(("include_removed", include_removed)).await?.check()?;
        let rows: Vec<Value> = result.take(0)?;
        Ok(rows.first().and_then(|v| v["count"].as_u64()).unwrap_or(0) as usize)
    }
    pub(crate) async fn page_cache(&self, source: &str) -> Result<Arc<dyn PageCache>> {
        // Cap the additional seed set independently of a source's eventual size.
        // Unlisted documents are never inferred absent or removed.
        let mut result = self.db.query("SELECT VALUE canonical_url FROM document WHERE source = type::record('source', $source) ORDER BY canonical_url LIMIT 10000;")
            .bind(("source", source.to_owned())).await?.check()?;
        let urls: Vec<String> = result.take(0)?;
        Ok(Arc::new(StoredPageCache {
            db: self.db.clone(),
            source: source.into(),
            urls,
        }))
    }

    pub(crate) async fn recrawl_snapshot(
        &self,
        source: &str,
        url: &Url,
    ) -> Result<Option<RecrawlSnapshot>> {
        let mut result = self.db.query("SELECT data.indexing AS indexing, data.revalidation AS revalidation FROM type::record('document', [$source, $url]);")
            .bind(("source", source.to_owned())).bind(("url", url.to_string())).await?.check()?;
        let rows: Vec<Value> = result.take(0)?;
        rows.into_iter()
            .next()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }

    /// Current availability and last validation, separate from content fetch provenance.
    pub async fn document_status(&self, source: &str, url: &Url) -> Result<Option<Value>> {
        let mut url = url.clone();
        url.set_fragment(None);
        let mut result = self.db.query("SELECT canonical_url AS url, data.revalidation AS revalidation, data.indexing AS indexing FROM type::record('document', [$source, $url]);")
            .bind(("source", source.to_owned())).bind(("url", url.to_string())).await?.check()?;
        let rows: Vec<Value> = result.take(0)?;
        Ok(rows.into_iter().next())
    }

    pub async fn source_status(&self, source: &str) -> Result<Value> {
        let registered = self.get_source(source).await?.context("source not found")?;
        let mut result = self.db.query("BEGIN TRANSACTION; SELECT count() AS count FROM document WHERE source = type::record('source', $source) GROUP ALL; SELECT count() AS count FROM document WHERE source = type::record('source', $source) AND data.revalidation.availability = 'removed' GROUP ALL; SELECT VALUE data FROM crawl_run WHERE source = type::record('source', $source) ORDER BY data.started_at.secs_since_epoch DESC, data.started_at.nanos_since_epoch DESC LIMIT 1; COMMIT TRANSACTION;")
            .bind(("source", source.to_owned())).await?.check()?;
        let all: Vec<Value> = result.take(1)?;
        let removed: Vec<Value> = result.take(2)?;
        let runs: Vec<Value> = result.take(3)?;
        let total = all.first().and_then(|v| v["count"].as_u64()).unwrap_or(0);
        let removed = removed
            .first()
            .and_then(|v| v["count"].as_u64())
            .unwrap_or(0);
        Ok(
            json!({"source":registered,"documents":total,"active_documents":total-removed,
            "removed_documents":removed,"latest_run":runs.into_iter().next()}),
        )
    }
}

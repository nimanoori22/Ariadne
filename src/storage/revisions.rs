//! Immutable revisions are an application-owned archive, separate from live indexes.
use super::*;
use crate::retrieval::RevisionSelector;
impl KnowledgeStore {
    pub async fn list_revisions(
        &self,
        source: &str,
        url: &Url,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Value> {
        ensure!(
            !source.trim().is_empty()
                && source.len() <= 1024
                && (1..=100).contains(&limit)
                && after.is_none_or(|n| n <= i64::MAX as u64),
            "invalid revision page"
        );
        crate::crawler::CrawlScope::new(url.clone(), url.path())?;
        ensure!(url.as_str().len() <= 4096, "invalid revision URL");
        let mut url = url.clone();
        url.set_fragment(None);
        let mut result=self.db.query("BEGIN TRANSACTION; LET $doc=type::record('document',[$source,$url]); SELECT VALUE {revision_id:revision_id,sequence:sequence,stored_at:stored_at,title:data.title,indexing:data.indexing,crawl_id:data.page.crawl_id,crawled_at:data.page.fetched_at,chunk_count:chunk_count,section_count:section_count,is_current:revision_id=$doc.current_revision} FROM document_revision WITH INDEX revisions_by_document WHERE document=$doc AND sequence > $after ORDER BY sequence LIMIT $limit; COMMIT TRANSACTION;")
            .bind(("source",source.to_owned())).bind(("url",url.to_string())).bind(("after",after.unwrap_or(0))).bind(("limit",limit+1)).await?.check()?;
        let mut revisions: Vec<Value> = result.take(2)?;
        let more = revisions.len() > limit;
        revisions.truncate(limit);
        let cursor = if more {
            revisions.last().and_then(|r| r["sequence"].as_u64())
        } else {
            None
        };
        Ok(
            json!({"source_id":source,"document_url":url,"revisions":revisions,"next_cursor":cursor}),
        )
    }
    pub async fn knowledge_revision(
        &self,
        revision: &RevisionSelector,
        section: Option<usize>,
        max_chars: usize,
    ) -> Result<Option<Value>> {
        revision.validate()?;
        ensure!(
            (1..=20000).contains(&max_chars),
            "invalid document text budget"
        );
        let mut url = revision.document_url.clone();
        url.set_fragment(None);
        let mut result = self
            .db
            .query(include_str!("read_revision.surql"))
            .bind(("source", revision.source_id.clone()))
            .bind(("url", url.to_string()))
            .bind(("revision_id", revision.revision_id.clone()))
            .bind(("section", section))
            .bind(("max_chars", max_chars))
            .await?
            .check()?;
        let docs: Vec<Value> = result.take(3)?;
        let Some(metadata) = docs.into_iter().next() else {
            return Ok(None);
        };
        let chunks: Vec<Value> = result.take(4)?;
        let sections: Vec<Value> = result.take(5)?;
        super::knowledge::render_document(
            &revision.source_id,
            &url,
            section,
            max_chars,
            metadata,
            chunks,
            sections,
        )
    }
    /// Explicit inspection API; unlike agent reads this includes retained raw HTML.
    pub async fn get_revision(
        &self,
        revision: &RevisionSelector,
    ) -> Result<Option<ExtractedDocument>> {
        revision.validate()?;
        let mut url = revision.document_url.clone();
        url.set_fragment(None);
        let mut result=self.db.query("BEGIN TRANSACTION; LET $rev=type::record('document_revision',$revision_id); SELECT VALUE data FROM $rev WHERE document=type::record('document',[$source,$url]); SELECT VALUE data FROM revision_section WHERE revision=$rev ORDER BY sequence; COMMIT TRANSACTION;")
            .bind(("source",revision.source_id.clone())).bind(("url",url.to_string())).bind(("revision_id",revision.revision_id.clone())).await?.check()?;
        let docs: Vec<Value> = result.take(2)?;
        let Some(mut data) = docs.into_iter().next() else {
            return Ok(None);
        };
        data["sections"] = serde_json::to_value(result.take::<Vec<Value>>(3)?)?;
        Ok(Some(serde_json::from_value(data)?))
    }
}

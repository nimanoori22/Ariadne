use super::*;
use crate::{
    chunking::Chunk,
    retrieval::{KnowledgeHit, RankedChunk},
};

#[derive(Debug, Deserialize)]
pub(crate) struct ContextRow {
    pub chunk: Chunk,
    pub text_truncated: bool,
    #[serde(default)]
    pub revision_id: Option<String>,
}
impl KnowledgeStore {
    /// Use the pinned engine's built-in rank fusion, keeping ranking independent
    /// of record layout and avoiding another implementation of its formula.
    pub async fn fuse_ranks(
        &self,
        lists: &[Vec<String>],
        constant: usize,
    ) -> Result<Vec<RankedChunk>> {
        ensure!(
            lists.len() <= 8 && lists.iter().all(|l| l.len() <= 50) && constant <= 10000,
            "rank fusion exceeds bounds"
        );
        let limit = lists.iter().map(Vec::len).sum::<usize>();
        if limit == 0 {
            return Ok(vec![]);
        }
        let rows: Vec<Vec<Value>> = lists
            .iter()
            .map(|list| list.iter().map(|id| json!({"id":id})).collect())
            .collect();
        let mut result = self
            .db
            .query("RETURN search::rrf($lists, $limit, $constant);")
            .bind(("lists", rows))
            .bind(("limit", limit))
            .bind(("constant", constant))
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Into::into))
            .collect()
    }

    pub(crate) async fn context_chunks(
        &self,
        hits: &[KnowledgeHit],
        radius: usize,
        max_chars: usize,
    ) -> Result<Vec<Vec<ContextRow>>> {
        ensure!(
            hits.len() <= 50 && radius <= 3 && (1..=20000).contains(&max_chars),
            "context request exceeds bounds"
        );
        let matches: Vec<Value> = hits.iter().map(|h| json!({"id":h.chunk_id,"hash":h.content_sha256,"source":h.source_id,"url":h.document_url,"revision_id":h.revision_id})).collect();
        let mut result = self
            .db
            .query(include_str!("context.surql"))
            .bind(("hits", matches))
            .bind(("radius", radius))
            .bind(("max_chars", max_chars))
            .await?
            .check()?;
        let values: Vec<Value> = result.take(1)?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Into::into))
            .collect()
    }
}

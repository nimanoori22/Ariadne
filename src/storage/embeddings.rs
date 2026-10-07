use super::*;
use crate::embeddings::{EmbeddingCoverage, EmbeddingSpace, EmbeddingState};
use surrealkit::TemplateVars;

static VECTOR_SCHEMA: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
    path: "database/schema/vector_space.surql",
    sql: include_str!("../../database/schema/vector_space.surql"),
}];

impl KnowledgeStore {
    async fn sync_embedding_schema(&self, space: &EmbeddingSpace) -> Result<()> {
        space.validate()?;
        // Only a generated SHA-256 identifier and validated integer are template
        // substitutions; provider/model names never become query syntax.
        Sync::embedded(VECTOR_SCHEMA)
            .module(space.id())?
            .prune(false)
            .vars(TemplateVars {
                vars: [
                    ("TABLE".into(), space.table()),
                    ("DIMENSIONS".into(), space.dimensions.to_string()),
                ]
                .into(),
            })
            .run(&self.db)
            .await
            .context("apply model-scoped vector schema")?;
        Ok(())
    }

    pub(crate) async fn restore_embedding_schemas(&self) -> Result<()> {
        for space in self.list_embedding_spaces().await? {
            self.sync_embedding_schema(&space).await?;
        }
        Ok(())
    }

    pub async fn list_embedding_spaces(&self) -> Result<Vec<EmbeddingSpace>> {
        let mut result = self
            .db
            .query("SELECT VALUE data FROM embedding_space ORDER BY id;")
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .map(|value| serde_json::from_value(value).map_err(Into::into))
            .collect()
    }

    pub(crate) async fn has_embedding_space(&self, space: &EmbeddingSpace) -> Result<bool> {
        let mut result = self
            .db
            .query("SELECT VALUE data FROM type::record('embedding_space', $id);")
            .bind(("id", space.id()))
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        if let Some(value) = values.into_iter().next() {
            let saved: EmbeddingSpace = serde_json::from_value(value)?;
            ensure!(&saved == space, "embedding space identity mismatch");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) async fn ensure_embedding_space(&self, space: &EmbeddingSpace) -> Result<()> {
        space.validate()?;
        if self.has_embedding_space(space).await? {
            return Ok(());
        }
        self.sync_embedding_schema(space).await?;
        self.db.query("UPSERT type::record('embedding_space', $id) SET table_name = $table, data = $space;")
            .bind(("id", space.id())).bind(("table", space.table())).bind(("space", serde_json::to_value(space)?)).await?.check()?;
        Ok(())
    }

    pub(crate) async fn embedding_chunks(
        &self,
        source: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<Vec<Chunk>> {
        let mut result = self.db.query("SELECT VALUE data FROM chunk WHERE data.source_id = $source AND data.id > $cursor ORDER BY data.id LIMIT $limit;")
            .bind(("source", source.to_owned())).bind(("cursor", cursor.to_owned())).bind(("limit", limit)).await?.check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Into::into))
            .collect()
    }

    pub(crate) async fn prepare_embedding(
        &self,
        space: &EmbeddingSpace,
        chunk: &Chunk,
    ) -> Result<bool> {
        let mut result = self
            .transaction(
                include_str!("prepare_embedding.surql"),
                json!({
                    "table":space.table(),"chunk_id":chunk.id,"hash":chunk.content_sha256,
                    "now":SystemTime::now()
                }),
            )
            .await
            .context("prepare embedding")?;
        // The pinned SDK retains BEGIN/LET/IF/COMMIT result slots. Both
        // scripts deliberately return their decision as statement 7.
        result
            .take::<Option<bool>>(7)?
            .context("embedding preparation missing result")
    }

    pub(crate) async fn complete_embedding(
        &self,
        space: &EmbeddingSpace,
        chunk: &Chunk,
        vector: Option<Vec<f64>>,
        error: Option<String>,
    ) -> Result<bool> {
        let success = vector.is_some();
        let mut result = self
            .transaction(
                include_str!("complete_embedding.surql"),
                json!({
                    "table":space.table(),"chunk_id":chunk.id,"hash":chunk.content_sha256,
                    "now":SystemTime::now(),"success":success,"vector":vector.unwrap_or_default(),
                    "error":error
                }),
            )
            .await
            .context("complete embedding")?;
        result
            .take::<Option<bool>>(7)?
            .context("embedding completion missing result")
    }

    /// Missing work is represented by absence of a state for a current chunk.
    /// Pending, failed and stale records survive restart for diagnosis/retry.
    pub async fn embedding_states(
        &self,
        space: &EmbeddingSpace,
        source: &str,
    ) -> Result<Vec<EmbeddingState>> {
        space.validate()?;
        if !self.has_embedding_space(space).await? {
            return Ok(Vec::new());
        }
        let mut result = self.db.query("SELECT VALUE data FROM type::table($table) WHERE source_id = $source ORDER BY data.chunk_id;")
            .bind(("table", space.table())).bind(("source", source.to_owned())).await?.check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Into::into))
            .collect()
    }

    pub async fn embedding_coverage(
        &self,
        space: &EmbeddingSpace,
        source: &str,
    ) -> Result<EmbeddingCoverage> {
        space.validate()?;
        ensure!(self.get_source(source).await?.is_some(), "unknown source");
        if !self.has_embedding_space(space).await? {
            let mut result = self
                .db
                .query(
                    "SELECT count() AS count FROM chunk WHERE data.source_id = $source GROUP ALL;",
                )
                .bind(("source", source.to_owned()))
                .await?
                .check()?;
            let counts: Vec<Value> = result.take(0)?;
            let count = counts
                .first()
                .and_then(|v| v["count"].as_u64())
                .unwrap_or(0) as usize;
            return Ok(EmbeddingCoverage {
                current_chunks: count,
                missing: count,
                ..Default::default()
            });
        }
        // Both status and chunk existence share a snapshot. A missing or stale
        // derivative never counts as current ready data.
        let sql = "BEGIN TRANSACTION; SELECT VALUE {status: type::record($table, data.id).data.status, valid: type::record($table, data.id).data.content_sha256 = data.content_sha256} FROM chunk WHERE data.source_id = $source; SELECT count() AS count FROM type::table($table) WHERE source_id = $source AND data.status = 'stale' GROUP ALL; COMMIT TRANSACTION;";
        let mut result = self
            .db
            .query(sql)
            .bind(("table", space.table()))
            .bind(("source", source.to_owned()))
            .await?
            .check()?;
        let current: Vec<Value> = result.take(1)?;
        let stale: Vec<Value> = result.take(2)?;
        let mut coverage = EmbeddingCoverage {
            current_chunks: current.len(),
            stale: stale.first().and_then(|v| v["count"].as_u64()).unwrap_or(0) as usize,
            ..Default::default()
        };
        for state in current {
            match (state["valid"].as_bool(), state["status"].as_str()) {
                (Some(true), Some("ready")) => coverage.ready += 1,
                (Some(true), Some("pending")) => coverage.pending += 1,
                (Some(true), Some("failed")) => coverage.failed += 1,
                _ => coverage.missing += 1,
            }
        }
        Ok(coverage)
    }

    pub(crate) async fn vector_search(
        &self,
        space: &EmbeddingSpace,
        vector: &[f64],
        query: &crate::retrieval::SearchQuery,
    ) -> Result<Vec<crate::retrieval::KnowledgeHit>> {
        let (score, candidates) = if query.source_id.is_some() {
            // On the pinned engine, sparse filtered HNSW queries did not fill
            // all K slots in our duplicate-vector fixture. Exact scoring within
            // the indexed source partition preserves recall and filter scope.
            (
                "vector::similarity::cosine(vector, $vector)",
                "WITH INDEX by_source WHERE source_id = $source AND vector != NONE".to_owned(),
            )
        } else {
            (
                "1.0 - vector::distance::knn()",
                format!(
                    "WITH INDEX vector_search WHERE vector <|{}, 100|> $vector",
                    query.limit
                ),
            )
        };
        // Identifiers are application-generated; only validated numeric bounds
        // enter the KNN syntax. Text, vectors and filters are bound parameters.
        let sql = format!(
            "{}, {score} AS score FROM {} {candidates} AND data.status = 'ready' AND data.content_sha256 = chunk.data.content_sha256 ORDER BY score DESC, source_id ASC, document_url ASC, sequence ASC, chunk_id ASC LIMIT $limit TIMEOUT 10s;",
            include_str!("vector_projection.surql"),
            space.table()
        );
        let mut result = self
            .db
            .query(sql)
            .bind(("vector", vector.to_vec()))
            .bind(("source", query.source_id.clone().unwrap_or_default()))
            .bind(("space", serde_json::to_value(space)?))
            .bind(("limit", query.limit))
            .bind(("max_text_chars", query.max_text_chars))
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .map(|v| {
                let mut hit: crate::retrieval::KnowledgeHit = serde_json::from_value(v)?;
                ensure!(
                    hit.score.is_finite() && (-1.000001..=1.000001).contains(&hit.score),
                    "invalid cosine similarity"
                );
                hit.score = hit.score.clamp(-1.0, 1.0);
                Ok(hit)
            })
            .collect()
    }
}

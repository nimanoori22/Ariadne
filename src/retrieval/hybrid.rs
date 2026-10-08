use super::{KnowledgeHit, MatchKind, SearchQuery, search, vector_search};
use crate::{embeddings::EmbeddingProvider, storage::KnowledgeStore};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FusionEvidence {
    pub strategy: String,
    pub lexical_rank: Option<usize>,
    pub vector_rank: Option<usize>,
    pub lexical_score: Option<f64>,
    pub vector_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_rank: Option<usize>,
}
#[derive(Debug, Deserialize)]
pub struct RankedChunk {
    pub id: String,
    pub rrf_score: f64,
}

/// Strategies operate on ordered, deduplicated candidate identities. Their
/// output is checked against the supplied candidates before returning knowledge.
#[allow(async_fn_in_trait)]
pub trait HybridRanker {
    fn name(&self) -> &str;
    async fn rank(&self, store: &KnowledgeStore, lists: &[Vec<String>])
    -> Result<Vec<RankedChunk>>;
}

#[derive(Debug, Clone, Copy)]
pub struct ReciprocalRankFusion {
    pub constant: usize,
}
impl Default for ReciprocalRankFusion {
    fn default() -> Self {
        Self { constant: 60 }
    }
}
impl HybridRanker for ReciprocalRankFusion {
    fn name(&self) -> &str {
        "reciprocal_rank_fusion"
    }
    async fn rank(
        &self,
        store: &KnowledgeStore,
        lists: &[Vec<String>],
    ) -> Result<Vec<RankedChunk>> {
        ensure!(self.constant <= 10000, "RRF constant must be at most 10000");
        store.fuse_ranks(lists, self.constant).await
    }
}

pub async fn hybrid_search<P: EmbeddingProvider>(
    store: &KnowledgeStore,
    provider: &P,
    query: SearchQuery,
) -> Result<Vec<KnowledgeHit>> {
    hybrid_search_with_ranker(store, provider, query, &ReciprocalRankFusion::default()).await
}

pub async fn hybrid_search_with_ranker<P: EmbeddingProvider, R: HybridRanker>(
    store: &KnowledgeStore,
    provider: &P,
    query: SearchQuery,
    ranker: &R,
) -> Result<Vec<KnowledgeHit>> {
    query.validate()?;
    let mut candidates = query.clone();
    candidates.limit = (query.limit * 4).clamp(20, 50);
    let mut semantic = candidates.clone();
    semantic.mode = super::SearchMode::Auto;
    let (lexical, vector) = tokio::try_join!(
        search(store, candidates),
        vector_search(store, provider, semantic)
    )?;
    fuse_candidates(store, &lexical, &vector, &[], query.limit, ranker).await
}

pub(crate) async fn fuse_candidates<R: HybridRanker>(
    store: &KnowledgeStore,
    lexical: &[KnowledgeHit],
    vector: &[KnowledgeHit],
    graph: &[KnowledgeHit],
    limit: usize,
    ranker: &R,
) -> Result<Vec<KnowledgeHit>> {
    let mut by_id: HashMap<String, KnowledgeHit> = HashMap::new();
    for (component, hits) in [(0, lexical), (1, vector), (2, graph)] {
        for (index, hit) in hits.iter().enumerate() {
            if let Some(saved) = by_id.get(&hit.chunk_id) {
                // Concurrent replacement must not fuse inconsistent versions.
                ensure!(
                    saved.source_id == hit.source_id
                        && saved.document_url == hit.document_url
                        && saved.content_sha256 == hit.content_sha256,
                    "candidate changed during hybrid search; retry"
                );
            }
            let saved = by_id.entry(hit.chunk_id.clone()).or_insert_with(|| {
                let mut value = hit.clone();
                value.fusion = Some(FusionEvidence {
                    strategy: ranker.name().into(),
                    lexical_rank: None,
                    vector_rank: None,
                    lexical_score: None,
                    vector_score: None,
                    graph_rank: None,
                });
                value
            });
            let evidence = saved.fusion.as_mut().unwrap();
            if component == 2 {
                evidence.graph_rank = Some(index + 1);
                saved.graph = hit.graph.clone();
            } else if component == 1 {
                evidence.vector_rank = Some(index + 1);
                evidence.vector_score = Some(hit.score);
                saved.embedding_space = hit.embedding_space.clone();
            } else {
                evidence.lexical_rank = Some(index + 1);
                evidence.lexical_score = Some(hit.score);
            }
        }
    }
    if by_id.is_empty() {
        return Ok(vec![]);
    }
    let mut lists = vec![
        lexical.iter().map(|h| h.chunk_id.clone()).collect(),
        vector.iter().map(|h| h.chunk_id.clone()).collect(),
    ];
    if !graph.is_empty() {
        lists.push(graph.iter().map(|h| h.chunk_id.clone()).collect());
    }
    let ranked = ranker.rank(store, &lists).await?;
    let mut hits = Vec::new();
    for rank in ranked {
        ensure!(rank.rrf_score.is_finite(), "invalid fused score");
        let mut hit = by_id
            .remove(&rank.id)
            .ok_or_else(|| anyhow::anyhow!("ranker returned unknown or repeated candidate"))?;
        hit.score = rank.rrf_score;
        hit.match_kind = if graph.is_empty() {
            MatchKind::Hybrid
        } else {
            MatchKind::Graph
        };
        hits.push(hit);
    }
    // Request the full bounded union from the engine so arbitrary ties at its
    // top-k boundary cannot change membership. Resolve ties before truncating.
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.source_id.cmp(&b.source_id))
            .then_with(|| a.document_url.as_str().cmp(b.document_url.as_str()))
            .then_with(|| a.sequence.cmp(&b.sequence))
            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
    });
    hits.truncate(limit);
    Ok(hits)
}

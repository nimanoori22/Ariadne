//! Opt-in one-hop graph candidates, fused with the existing retrieval lists.
use super::{
    KnowledgeHit, ReciprocalRankFusion, SearchQuery, hybrid::fuse_candidates, search, vector_search,
};
use crate::{embeddings::EmbeddingProvider, storage::KnowledgeStore};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use url::Url;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GraphOptions {
    pub max_seeds: usize,
    pub max_edges_per_seed: usize,
    pub max_chunks_per_edge: usize,
    pub max_candidates: usize,
    pub links: bool,
    pub entities: bool,
}
impl Default for GraphOptions {
    fn default() -> Self {
        Self {
            max_seeds: 4,
            max_edges_per_seed: 4,
            max_chunks_per_edge: 2,
            max_candidates: 20,
            links: true,
            entities: true,
        }
    }
}
impl GraphOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=8).contains(&self.max_seeds),
            "graph seeds must be 1..8"
        );
        ensure!(
            (1..=8).contains(&self.max_edges_per_seed),
            "graph edges per seed must be 1..8"
        );
        ensure!(
            (1..=3).contains(&self.max_chunks_per_edge),
            "graph chunks per edge must be 1..3"
        );
        ensure!(
            (1..=50).contains(&self.max_candidates),
            "graph candidates must be 1..50"
        );
        ensure!(
            self.links || self.entities,
            "enable links or entities for graph assistance"
        );
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphRelation {
    OutgoingLink,
    IncomingLink,
    SharedEntity,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphPath {
    pub relation: GraphRelation,
    pub seed_chunk_id: String,
    pub seed_content_sha256: String,
    pub seed_url: Url,
    pub link_source_url: Option<Url>,
    pub link_url: Option<Url>,
    pub entity: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphEvidence {
    pub paths: Vec<GraphPath>,
    pub paths_truncated: bool,
}
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphReport {
    pub index_version: String,
    pub seeds: usize,
    pub seeds_truncated: bool,
    pub skipped_stale_seeds: usize,
    pub unindexed_seeds: usize,
    pub truncated_seed_indexes: usize,
    pub traversal_truncated: bool,
    pub candidates_truncated: bool,
    pub candidates: usize,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphSearchResponse {
    pub hits: Vec<KnowledgeHit>,
    pub graph: GraphReport,
}
#[derive(Debug, Deserialize)]
pub(crate) struct GraphRow {
    #[serde(flatten)]
    pub hit: KnowledgeHit,
    pub graph_path: GraphPath,
}
#[derive(Debug, Deserialize)]
pub(crate) struct GraphSeedRow {
    pub status: String,
    #[serde(default)]
    pub index_truncated: bool,
    #[serde(default)]
    pub edges_truncated: bool,
    #[serde(default)]
    pub groups: Vec<Vec<GraphRow>>,
}

#[derive(Clone, Copy)]
enum Base {
    Lexical,
    Vector,
    Hybrid,
}

fn candidates(query: &SearchQuery) -> SearchQuery {
    let mut result = query.clone();
    result.limit = (query.limit * 4).clamp(20, 50);
    result
}
/// Lexical graph assistance remains usable without an embedding provider.
pub async fn graph_search(
    store: &KnowledgeStore,
    query: SearchQuery,
    options: GraphOptions,
) -> Result<GraphSearchResponse> {
    query.validate()?;
    options.validate()?;
    let lexical = search(store, candidates(&query)).await?;
    expand_and_fuse(store, &lexical, &[], &query, options, Base::Lexical).await
}
pub async fn graph_vector_search<P: EmbeddingProvider>(
    store: &KnowledgeStore,
    provider: &P,
    query: SearchQuery,
    options: GraphOptions,
) -> Result<GraphSearchResponse> {
    query.validate()?;
    options.validate()?;
    let vector = vector_search(store, provider, candidates(&query)).await?;
    expand_and_fuse(store, &[], &vector, &query, options, Base::Vector).await
}
pub async fn graph_hybrid_search<P: EmbeddingProvider>(
    store: &KnowledgeStore,
    provider: &P,
    query: SearchQuery,
    options: GraphOptions,
) -> Result<GraphSearchResponse> {
    query.validate()?;
    options.validate()?;
    let lexical_query = candidates(&query);
    let mut vector_query = lexical_query.clone();
    vector_query.mode = super::SearchMode::Auto;
    let (lexical, vector) = tokio::try_join!(
        search(store, lexical_query),
        vector_search(store, provider, vector_query)
    )?;
    expand_and_fuse(store, &lexical, &vector, &query, options, Base::Hybrid).await
}

async fn expand_and_fuse(
    store: &KnowledgeStore,
    lexical: &[KnowledgeHit],
    vector: &[KnowledgeHit],
    query: &SearchQuery,
    options: GraphOptions,
    base: Base,
) -> Result<GraphSearchResponse> {
    let ranker = ReciprocalRankFusion::default();
    let seeds = fuse_candidates(store, lexical, vector, &[], options.max_seeds, &ranker).await?;
    let (graph, mut report, stale) = expand_candidates(store, &seeds, query, options).await?;
    report.seeds_truncated = lexical
        .iter()
        .chain(vector)
        .map(|h| &h.chunk_id)
        .collect::<HashSet<_>>()
        .len()
        > seeds.len();
    // Don't return a seed the graph snapshot has proved obsolete.
    let lexical: Vec<_> = lexical
        .iter()
        .filter(|h| !stale.contains(&h.chunk_id))
        .cloned()
        .collect();
    let vector: Vec<_> = vector
        .iter()
        .filter(|h| !stale.contains(&h.chunk_id))
        .cloned()
        .collect();
    let mut hits = fuse_candidates(store, &lexical, &vector, &graph, query.limit, &ranker).await?;
    // With no eligible graph evidence preserve ordinary mode and score semantics.
    if graph.is_empty() && matches!(base, Base::Lexical) {
        hits = lexical;
        hits.truncate(query.limit);
    } else if graph.is_empty() && matches!(base, Base::Vector) {
        hits = vector;
        hits.truncate(query.limit);
    }
    Ok(GraphSearchResponse {
        hits,
        graph: report,
    })
}

pub(crate) async fn expand_candidates(
    store: &KnowledgeStore,
    seeds: &[KnowledgeHit],
    query: &SearchQuery,
    options: GraphOptions,
) -> Result<(Vec<KnowledgeHit>, GraphReport, HashSet<String>)> {
    options.validate()?;
    query.validate()?;
    ensure!(seeds.len() <= options.max_seeds, "too many graph seeds");
    let snapshots = store.graph_neighbors(seeds, query, options).await?;
    ensure!(
        snapshots.len() == seeds.len(),
        "graph snapshot count mismatch"
    );
    let mut report = GraphReport {
        index_version: crate::graph::GRAPH_VERSION.into(),
        seeds: seeds.len(),
        ..Default::default()
    };
    let mut stale = HashSet::new();
    let mut by_id: HashMap<String, KnowledgeHit> = HashMap::new();
    let mut order = Vec::new();
    // Interleave edge groups so a hub cannot occupy the whole candidate budget.
    let mut groups = Vec::new();
    for (seed, snapshot) in seeds.iter().zip(snapshots) {
        match snapshot.status.as_str() {
            "stale" => {
                report.skipped_stale_seeds += 1;
                stale.insert(seed.chunk_id.clone());
                continue;
            }
            "unindexed" => {
                report.unindexed_seeds += 1;
                continue;
            }
            "ready" => {}
            _ => anyhow::bail!("invalid graph snapshot status"),
        }
        report.truncated_seed_indexes += usize::from(snapshot.index_truncated);
        report.traversal_truncated |= snapshot.edges_truncated;
        for mut rows in snapshot.groups {
            report.traversal_truncated |= rows.len() > options.max_chunks_per_edge;
            rows.truncate(options.max_chunks_per_edge);
            groups.push(rows.into_iter());
        }
    }
    for _ in 0..options.max_chunks_per_edge {
        for group in &mut groups {
            let Some(row) = group.next() else {
                continue;
            };
            let id = row.hit.chunk_id.clone();
            if let Some(hit) = by_id.get_mut(&id) {
                ensure!(
                    hit.content_sha256 == row.hit.content_sha256
                        && hit.source_id == row.hit.source_id
                        && hit.document_url == row.hit.document_url,
                    "graph candidate changed; retry"
                );
                let evidence = hit.graph.as_mut().unwrap();
                if !evidence.paths.contains(&row.graph_path) {
                    if evidence.paths.len() < 4 {
                        evidence.paths.push(row.graph_path);
                    } else {
                        evidence.paths_truncated = true;
                    }
                }
            } else {
                order.push(id.clone());
                let mut hit = row.hit;
                hit.graph = Some(GraphEvidence {
                    paths: vec![row.graph_path],
                    paths_truncated: false,
                });
                by_id.insert(id, hit);
            }
        }
    }
    report.candidates_truncated = order.len() > options.max_candidates;
    order.truncate(options.max_candidates);
    let graph: Vec<_> = order
        .into_iter()
        .map(|id| by_id.remove(&id).unwrap())
        .collect();
    report.candidates = graph.len();
    Ok((graph, report, stale))
}

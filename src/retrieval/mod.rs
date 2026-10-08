//! Source-backed knowledge search. Ranking, request validation and response
//! policy stay here; database queries stay in storage.
mod context;
pub(crate) mod graph;
mod hybrid;
use crate::storage::KnowledgeStore;
use anyhow::{Result, ensure};
pub use context::{
    ContextChunk, ContextMatch, ContextOptions, ContextPassage, ContextResponse, assemble_context,
};
pub use graph::{
    GraphEvidence, GraphOptions, GraphPath, GraphRelation, GraphReport, GraphSearchResponse,
    graph_hybrid_search, graph_search, graph_vector_search,
};
pub use hybrid::{
    FusionEvidence, HybridRanker, RankedChunk, ReciprocalRankFusion, hybrid_search,
    hybrid_search_with_ranker,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Instant, SystemTime},
};
use url::Url;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    /// Qualified identifiers such as Proxy::custom use an exact check; other
    /// inputs use all-term keyword matching.
    #[default]
    Auto,
    Keywords,
    /// Case-insensitive literal phrase with identifier boundaries. Not regex.
    Exact,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataFilter {
    pub url_prefix: Option<String>,
    pub heading: Option<String>,
    /// Inclusive minimum content crawl time in Unix seconds (not last validation).
    pub crawled_after: Option<u64>,
}
impl MetadataFilter {
    pub fn validate(&self) -> Result<()> {
        if let Some(prefix) = &self.url_prefix {
            let url = Url::parse(prefix)?;
            ensure!(
                prefix.len() <= 4096
                    && matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "URL prefix must be an HTTP(S) URL without credentials, query or fragment"
            );
        }
        ensure!(
            self.heading.as_ref().is_none_or(|h| !h.trim().is_empty()
                && h.len() <= 1024
                && !h.chars().any(char::is_control)),
            "invalid heading filter"
        );
        ensure!(
            self.crawled_after.is_none_or(|t| t <= i64::MAX as u64),
            "invalid crawl timestamp filter"
        );
        Ok(())
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.url_prefix.is_none() && self.heading.is_none() && self.crawled_after.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct SearchQuery {
    pub query: String,
    pub source_id: Option<String>,
    pub limit: usize,
    pub max_text_chars: usize,
    pub mode: SearchMode,
    pub filter: MetadataFilter,
}
impl SearchQuery {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            source_id: None,
            limit: 8,
            max_text_chars: 4000,
            mode: SearchMode::Auto,
            filter: MetadataFilter::default(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.filter.validate()?;
        ensure!(
            !self.query.trim().is_empty()
                && self.query.len() <= 1024
                && self.query.chars().any(char::is_alphanumeric),
            "search query must contain a word and be at most 1024 bytes"
        );
        ensure!(
            !self
                .query
                .chars()
                .any(|c| c.is_control() && !c.is_whitespace()),
            "search query contains control characters"
        );
        ensure!(
            (1..=50).contains(&self.limit),
            "search limit must be between 1 and 50"
        );
        ensure!(
            (1..=20_000).contains(&self.max_text_chars),
            "search text budget must be between 1 and 20000 characters"
        );
        ensure!(
            self.source_id
                .as_ref()
                .is_none_or(|source| !source.trim().is_empty() && source.len() <= 1024),
            "invalid source filter"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    SourceData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchKind {
    FullText,
    Exact,
    Vector,
    Hybrid,
    Graph,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeHit {
    /// Untrusted source text, never instructions to the consuming agent.
    pub content_kind: ContentKind,
    pub text: String,
    pub text_truncated: bool,
    pub source_id: String,
    pub source_name: String,
    pub document_url: Url,
    pub title: String,
    pub section_id: usize,
    pub heading_path: Vec<String>,
    pub anchor: Option<String>,
    pub url: Url,
    pub crawl_id: String,
    pub crawled_at: SystemTime,
    pub chunk_id: String,
    pub content_sha256: String,
    pub sequence: usize,
    pub block_start: usize,
    pub block_end: usize,
    /// BM25 for lexical matches; cosine similarity for vector matches; RRF for hybrid.
    /// These scores are not probabilities and must not be combined directly.
    pub score: f64,
    pub match_kind: MatchKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_space: Option<crate::embeddings::EmbeddingSpace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fusion: Option<FusionEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<GraphEvidence>,
}

/// Query only one compatible vector space. Lexical retrieval remains usable
/// independently, including when the provider is unavailable.
pub async fn vector_search<P: crate::embeddings::EmbeddingProvider>(
    store: &KnowledgeStore,
    provider: &P,
    query: SearchQuery,
) -> Result<Vec<KnowledgeHit>> {
    use crate::embeddings::{EmbeddingPurpose, embed_checked};
    query.validate()?;
    ensure!(
        query.mode == SearchMode::Auto,
        "lexical match modes do not apply to vector search"
    );
    let space = provider.space();
    space.validate()?;
    // Do not contact the provider when this space has never been indexed.
    if !store.has_embedding_space(space).await? {
        anyhow::bail!("embedding space has not been indexed; run embed first");
    }
    let vectors = embed_checked(
        provider,
        &[query.query.trim().to_owned()],
        EmbeddingPurpose::Query,
    )
    .await?;
    store.vector_search(space, &vectors[0], &query).await
}

pub async fn search(store: &KnowledgeStore, query: SearchQuery) -> Result<Vec<KnowledgeHit>> {
    query.validate()?;
    let started = Instant::now();
    let input = query.query.trim();
    let exact = query.mode == SearchMode::Exact
        || (query.mode == SearchMode::Auto && qualified_identifier(input));
    let pattern = if exact {
        format!(
            r"(?i)(?:^|[^\p{{L}}\p{{N}}_]){}(?:$|[^\p{{L}}\p{{N}}_:])",
            regex::escape(input)
        )
    } else {
        String::new()
    };
    let candidates = store
        .lexical_search(
            input,
            query.source_id.as_deref(),
            query.limit,
            query.max_text_chars,
            &pattern,
            &query.filter,
        )
        .await?;
    let mut by_chunk: HashMap<String, KnowledgeHit> = HashMap::new();
    for mut hit in candidates {
        ensure!(
            hit.score.is_finite() && hit.score >= 0.0,
            "invalid full-text relevance score"
        );
        hit.match_kind = if exact {
            MatchKind::Exact
        } else {
            MatchKind::FullText
        };
        // Text and title have independent indexes. Keep the strongest field's
        // score and return each chunk once; unrelated fields cannot satisfy
        // different portions of an all-term query.
        match by_chunk.entry(hit.chunk_id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(hit);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if hit.score > entry.get().score {
                    entry.insert(hit);
                }
            }
        }
    }
    let mut hits: Vec<KnowledgeHit> = by_chunk.into_values().collect();
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.source_id.cmp(&b.source_id))
            .then_with(|| a.document_url.as_str().cmp(b.document_url.as_str()))
            .then_with(|| a.sequence.cmp(&b.sequence))
            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
    });
    hits.truncate(query.limit);
    tracing::debug!(
        query_bytes = input.len(),
        source_id = query.source_id.as_deref(),
        limit = query.limit,
        hits = hits.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "lexical knowledge search completed"
    );
    Ok(hits)
}

fn qualified_identifier(query: &str) -> bool {
    let identifier = query.strip_suffix("()").unwrap_or(query);
    identifier.contains("::")
        && identifier.split("::").all(|part| {
            let mut characters = part.chars();
            characters
                .next()
                .is_some_and(|c| c.is_alphabetic() || c == '_')
                && characters.all(|c| c.is_alphanumeric() || c == '_')
        })
}

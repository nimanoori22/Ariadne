use super::{ContentKind, FusionEvidence, KnowledgeHit, MatchKind};
use crate::{chunking::Chunk, storage::KnowledgeStore};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};
use url::Url;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContextOptions {
    pub neighbor_chunks: usize,
    pub max_chunks: usize,
    pub max_total_chars: usize,
    pub max_chunk_chars: usize,
}
impl Default for ContextOptions {
    fn default() -> Self {
        Self {
            neighbor_chunks: 1,
            max_chunks: 50,
            max_total_chars: 12000,
            max_chunk_chars: 4000,
        }
    }
}
impl ContextOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.neighbor_chunks <= 3,
            "context neighbors must be at most 3"
        );
        ensure!(
            (1..=100).contains(&self.max_chunks),
            "context chunk limit must be 1..100"
        );
        ensure!(
            (1..=100000).contains(&self.max_total_chars),
            "context total budget must be 1..100000 characters"
        );
        ensure!(
            (1..=20000).contains(&self.max_chunk_chars),
            "context chunk budget must be 1..20000 characters"
        );
        Ok(())
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ContextMatch {
    pub chunk_id: String,
    pub score: f64,
    pub match_kind: MatchKind,
    pub fusion: Option<FusionEvidence>,
    pub text_omitted: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ContextChunk {
    pub content_kind: ContentKind,
    pub chunk_id: String,
    pub text: String,
    pub text_truncated: bool,
    pub source_id: String,
    pub document_url: Url,
    pub title: String,
    pub section_id: usize,
    pub heading_path: Vec<String>,
    pub url: Url,
    pub sequence: usize,
    pub block_start: usize,
    pub block_end: usize,
    pub content_sha256: String,
    pub crawl_id: String,
    pub crawled_at: SystemTime,
}
impl ContextChunk {
    fn from_chunk(chunk: Chunk, truncated: bool) -> Self {
        Self {
            content_kind: ContentKind::SourceData,
            chunk_id: chunk.id,
            text: chunk.text,
            text_truncated: truncated,
            source_id: chunk.source_id,
            document_url: chunk.document_url,
            title: chunk.title,
            section_id: chunk.section_id,
            heading_path: chunk.heading_path,
            url: chunk.source_url,
            sequence: chunk.sequence,
            block_start: chunk.block_start,
            block_end: chunk.block_end,
            content_sha256: chunk.content_sha256,
            crawl_id: chunk.crawl_id,
            crawled_at: chunk.crawled_at,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ContextPassage {
    pub source_id: String,
    pub source_name: String,
    pub document_url: Url,
    pub title: String,
    pub matches: Vec<ContextMatch>,
    pub chunks: Vec<ContextChunk>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ContextResponse {
    pub passages: Vec<ContextPassage>,
    pub skipped_stale_hits: usize,
    pub deduplicated_chunks: usize,
    pub omitted_chunks: usize,
    pub total_text_chars: usize,
    pub budget_exhausted: bool,
}

/// Read all requested neighborhoods in one transaction. Stale search hits are
/// skipped rather than silently expanding another version of a document.
pub async fn assemble_context(
    store: &KnowledgeStore,
    hits: &[KnowledgeHit],
    options: ContextOptions,
) -> Result<ContextResponse> {
    options.validate()?;
    ensure!(hits.len() <= 50, "context accepts at most 50 hits");
    let snapshots = store
        .context_chunks(hits, options.neighbor_chunks, options.max_chunk_chars)
        .await?;
    ensure!(
        snapshots.len() == hits.len(),
        "context snapshot missing result"
    );
    let mut response = ContextResponse {
        passages: vec![],
        skipped_stale_hits: 0,
        deduplicated_chunks: 0,
        omitted_chunks: 0,
        total_text_chars: 0,
        budget_exhausted: false,
    };
    let mut candidates = HashMap::new();
    let mut matched = Vec::new();
    let mut neighbors = Vec::new();
    let mut valid_hits = Vec::new();
    for (index, (hit, rows)) in hits.iter().zip(snapshots).enumerate() {
        let Some(matched_sequence) = rows
            .iter()
            .find(|r| r.chunk.id == hit.chunk_id)
            .map(|r| r.chunk.sequence)
        else {
            response.skipped_stale_hits += 1;
            continue;
        };
        valid_hits.push(hit);
        matched.push(hit.chunk_id.clone());
        for row in rows {
            let id = row.chunk.id.clone();
            let distance = row.chunk.sequence.abs_diff(matched_sequence);
            neighbors.push((distance, index, row.chunk.sequence, id.clone()));
            if candidates.insert(id, row).is_some() {
                response.deduplicated_chunks += 1;
            }
        }
    }
    neighbors.sort();
    matched.extend(neighbors.into_iter().map(|(_, _, _, id)| id));
    let mut included = HashSet::new();
    let mut groups: HashMap<(String, String), usize> = HashMap::new();
    let candidate_count = candidates.len();
    for id in matched {
        let Some(mut row) = candidates.remove(&id) else {
            continue;
        };
        let remaining = options
            .max_total_chars
            .saturating_sub(response.total_text_chars);
        if remaining == 0 || included.len() >= options.max_chunks {
            continue;
        }
        let count = row.chunk.text.chars().count();
        if count > remaining {
            row.chunk.text = row.chunk.text.chars().take(remaining).collect();
            row.text_truncated = true;
        }
        response.total_text_chars += row.chunk.text.chars().count();
        response.budget_exhausted |= row.text_truncated;
        included.insert(id);
        let key = (
            row.chunk.source_id.clone(),
            row.chunk.document_url.to_string(),
        );
        let index = *groups.entry(key).or_insert_with(|| {
            let hit = valid_hits
                .iter()
                .find(|h| {
                    h.source_id == row.chunk.source_id && h.document_url == row.chunk.document_url
                })
                .unwrap();
            let index = response.passages.len();
            response.passages.push(ContextPassage {
                source_id: hit.source_id.clone(),
                source_name: hit.source_name.clone(),
                document_url: hit.document_url.clone(),
                title: hit.title.clone(),
                matches: vec![],
                chunks: vec![],
            });
            index
        });
        response.passages[index]
            .chunks
            .push(ContextChunk::from_chunk(row.chunk, row.text_truncated));
    }
    for passage in &mut response.passages {
        passage.chunks.sort_by_key(|c| c.sequence);
        passage.matches = valid_hits
            .iter()
            .filter(|h| h.source_id == passage.source_id && h.document_url == passage.document_url)
            .map(|h| ContextMatch {
                chunk_id: h.chunk_id.clone(),
                score: h.score,
                match_kind: h.match_kind,
                fusion: h.fusion.clone(),
                text_omitted: !included.contains(&h.chunk_id),
            })
            .collect();
    }
    response.omitted_chunks = candidate_count - included.len();
    response.budget_exhausted |= response.omitted_chunks > 0;
    Ok(response)
}

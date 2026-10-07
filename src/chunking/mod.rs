//! Deterministic structure-based retrieval units. Character budgets are not
//! model token limits; embedding providers enforce their own input policy.
use crate::extraction::{ContentBlock, ExtractedDocument, Section};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, time::SystemTime};
use url::Url;

pub const NORMALIZATION_VERSION: &str = "structured-document-v1";
pub const CHUNKING_VERSION: &str = "section-blocks-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPolicy {
    /// Soft budget for Unicode scalar values in rendered Markdown, including
    /// heading context. Atomic blocks/example groups can exceed it explicitly.
    pub target_chars: usize,
}
impl Default for ChunkPolicy {
    fn default() -> Self {
        Self { target_chars: 2400 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentHashes {
    pub normalization_version: String,
    pub raw_sha256: String,
    pub normalized_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub hashes: ContentHashes,
    pub extraction_version: String,
    pub chunking_version: String,
    pub policy: ChunkPolicy,
    pub chunk_count: usize,
    pub oversized_chunk_count: usize,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentIndex {
    pub metadata: IndexMetadata,
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OversizedReason {
    HeadingContext,
    Code,
    Table,
    Paragraph,
    List,
    Note,
    Quote,
    ExampleGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// SHA-256 identity scoped to source, URL, section locator, content and policy.
    /// Crawl timestamps and global sequence are deliberately excluded.
    pub id: String,
    pub source_id: String,
    pub document_url: Url,
    pub title: String,
    pub section_id: usize,
    pub heading_path: Vec<String>,
    pub anchor: Option<String>,
    pub source_url: Url,
    pub crawl_id: String,
    pub crawled_at: SystemTime,
    pub sequence: usize,
    /// Half-open range in the source section's ordered content blocks.
    pub block_start: usize,
    pub block_end: usize,
    pub text: String,
    pub markdown: String,
    pub content_sha256: String,
    pub char_count: usize,
    /// Unknown until a particular embedding provider/tokenizer is selected.
    pub token_count: Option<usize>,
    pub oversized: Option<OversizedReason>,
}

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Raw bytes and structured semantics are different identities. Canonical URL,
/// HTTP metadata, timestamps, diagnostics and surrounding boilerplate are not
/// part of normalized content. Links and exact code whitespace are part of it.
pub fn content_hashes(document: &ExtractedDocument) -> Result<ContentHashes> {
    let normalized = serde_json::to_vec(&(
        NORMALIZATION_VERSION,
        &document.title,
        &document.sections,
        &document.links,
    ))?;
    Ok(ContentHashes {
        normalization_version: NORMALIZATION_VERSION.into(),
        raw_sha256: sha256(&document.page.raw_body),
        normalized_sha256: sha256(&normalized),
    })
}

pub fn index_document(document: &ExtractedDocument, policy: ChunkPolicy) -> Result<DocumentIndex> {
    ensure!(
        (1..=1_000_000).contains(&policy.target_chars),
        "chunk target must be between 1 and 1000000 characters"
    );
    ensure!(!document.sections.is_empty(), "document has no sections");
    let mut chunks = Vec::new();
    let mut paths: Vec<Vec<String>> = Vec::with_capacity(document.sections.len());
    let mut section_occurrences: HashMap<String, usize> = HashMap::new();
    for (position, section) in document.sections.iter().enumerate() {
        ensure!(
            section.id == position && section.parent_id.is_none_or(|parent| parent < position),
            "invalid section order or parent"
        );
        let mut path = section
            .parent_id
            .map(|parent| paths[parent].clone())
            .unwrap_or_default();
        if let Some(heading) = &section.heading {
            path.push(heading.clone());
        }
        paths.push(path.clone());
        let locator = serde_json::to_string(&(&section.anchor, &path))?;
        let section_occurrence = section_occurrences.entry(locator.clone()).or_default();
        let occurrence = *section_occurrence;
        *section_occurrence += 1;
        let prefix = heading_context(&path);
        let prefix_chars = prefix.chars().count();
        // Prefix sums keep packing linear even when a section has many tiny
        // blocks. Code/table rendering is measured once, never per candidate.
        let mut block_chars = vec![0usize];
        for block in &section.blocks {
            block_chars.push(
                block_chars.last().copied().unwrap_or_default() + block.markdown().chars().count(),
            );
        }
        let rendered_chars = |start: usize, end: usize| {
            let body = block_chars[end] - block_chars[start] + 2 * (end - start).saturating_sub(1);
            body + prefix_chars + usize::from(body > 0 && prefix_chars > 0) * 2
        };
        let units = semantic_units(&section.blocks);
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        let mut pending: Option<(usize, usize)> = None;
        for (start, end) in units {
            if let Some((pending_start, pending_end)) = pending {
                let combined = rendered_chars(pending_start, end);
                if combined <= policy.target_chars {
                    pending = Some((pending_start, end));
                } else {
                    ranges.push((pending_start, pending_end));
                    pending = Some((start, end));
                }
            } else {
                pending = Some((start, end));
            }
        }
        if let Some(range) = pending {
            ranges.push(range);
        }
        if ranges.is_empty() && section.heading.is_some() {
            ranges.push((0, 0));
        }
        let mut repeated_content: HashMap<String, usize> = HashMap::new();
        for (start, end) in ranges {
            let (text, markdown) = render(section, start, end, &prefix, &path);
            let content_sha256 = sha256(&serde_json::to_vec(&(
                CHUNKING_VERSION,
                &path,
                &section.blocks[start..end],
            ))?);
            let content_occurrence = repeated_content.entry(content_sha256.clone()).or_default();
            let id = sha256(&serde_json::to_vec(&(
                CHUNKING_VERSION,
                policy,
                &document.page.source_id,
                document.canonical_url.as_str(),
                &locator,
                occurrence,
                &content_sha256,
                *content_occurrence,
            ))?);
            *content_occurrence += 1;
            let mut source_url = document.canonical_url.clone();
            source_url.set_fragment(section.anchor.as_deref());
            let char_count = markdown.chars().count();
            let oversized = (char_count > policy.target_chars).then(|| {
                if prefix_chars > policy.target_chars {
                    OversizedReason::HeadingContext
                } else {
                    oversized_reason(section, start, end)
                }
            });
            chunks.push(Chunk {
                id,
                source_id: document.page.source_id.clone(),
                document_url: document.canonical_url.clone(),
                title: document.title.clone(),
                section_id: section.id,
                heading_path: path.clone(),
                anchor: section.anchor.clone(),
                source_url,
                crawl_id: document.page.crawl_id.clone(),
                crawled_at: document.page.fetched_at,
                sequence: chunks.len(),
                block_start: start,
                block_end: end,
                text,
                markdown,
                content_sha256,
                char_count,
                token_count: None,
                oversized,
            });
        }
    }
    let metadata = IndexMetadata {
        hashes: content_hashes(document)?,
        extraction_version: document.extraction_version.clone(),
        chunking_version: CHUNKING_VERSION.into(),
        policy,
        chunk_count: chunks.len(),
        oversized_chunk_count: chunks
            .iter()
            .filter(|chunk| chunk.oversized.is_some())
            .count(),
    };
    Ok(DocumentIndex { metadata, chunks })
}

fn semantic_units(blocks: &[ContentBlock]) -> Vec<(usize, usize)> {
    let mut units = Vec::new();
    let mut position = 0;
    while position < blocks.len() {
        let start = position;
        position += 1;
        // Paragraph explanations stay with immediately following examples. A
        // sequence of adjacent code blocks is one example unit as well.
        if matches!(
            blocks[start],
            ContentBlock::Paragraph(_) | ContentBlock::Code { .. }
        ) {
            while position < blocks.len() && matches!(blocks[position], ContentBlock::Code { .. }) {
                position += 1;
            }
        }
        units.push((start, position));
    }
    units
}

fn heading_context(path: &[String]) -> String {
    // Render via the existing inline Markdown escaping instead of inventing a
    // second escaping policy. Keep every ancestor visible in each chunk.
    path.iter()
        .enumerate()
        .map(|(index, heading)| {
            format!(
                "{} {}",
                "#".repeat((index + 1).min(6)),
                ContentBlock::Paragraph(vec![crate::extraction::Inline::Text(heading.clone())])
                    .markdown()
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn render(
    section: &Section,
    start: usize,
    end: usize,
    prefix: &str,
    path: &[String],
) -> (String, String) {
    let text = section.blocks[start..end]
        .iter()
        .map(ContentBlock::plain_text)
        .collect::<Vec<_>>()
        .join("\n\n");
    let markdown = section.blocks[start..end]
        .iter()
        .map(ContentBlock::markdown)
        .collect::<Vec<_>>()
        .join("\n\n");
    let heading_text = path.join("\n\n");
    let text = if text.is_empty() {
        heading_text
    } else if !heading_text.is_empty() {
        format!("{heading_text}\n\n{text}")
    } else {
        text
    };
    let markdown = if prefix.is_empty() {
        markdown
    } else if markdown.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix}\n\n{markdown}")
    };
    (text, markdown)
}

fn oversized_reason(section: &Section, start: usize, end: usize) -> OversizedReason {
    if start == end {
        return OversizedReason::HeadingContext;
    }
    if end - start > 1 {
        return OversizedReason::ExampleGroup;
    }
    match section.blocks[start] {
        ContentBlock::Paragraph(_) => OversizedReason::Paragraph,
        ContentBlock::Code { .. } => OversizedReason::Code,
        ContentBlock::List { .. } => OversizedReason::List,
        ContentBlock::Table { .. } => OversizedReason::Table,
        ContentBlock::Note { .. } => OversizedReason::Note,
        ContentBlock::Quote(_) => OversizedReason::Quote,
    }
}

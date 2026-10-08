//! Deterministic derived knowledge. Source text never becomes instructions.
use crate::{
    chunking::Chunk,
    extraction::{ContentBlock, ExtractedDocument, Inline},
};
use anyhow::{Context, Result, ensure};
use regex::Regex;
use serde::Serialize;
use std::{collections::BTreeSet, sync::LazyLock};
use url::Url;

pub const GRAPH_VERSION: &str = "document-links-rust-paths-v1";
const MAX_LINKS: usize = 4096;
const MAX_MENTIONS: usize = 8192;
static PATH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)+\b").unwrap()
});

#[derive(Debug, Serialize)]
pub(crate) struct Link {
    target_url: Url,
    url: Url,
    text: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct Mention {
    symbol: String,
    chunk_id: String,
    section_id: usize,
    url: Url,
}
#[derive(Debug, Serialize)]
pub(crate) struct DocumentGraph {
    version: &'static str,
    links: Vec<Link>,
    mentions: Vec<Mention>,
    truncated: bool,
}

fn symbols(text: &str, found: &mut BTreeSet<String>) {
    for capture in PATH.find_iter(text) {
        // Avoid extracting a valid-looking suffix from a longer Unicode name.
        let adjacent = |c: char| c.is_alphanumeric() || c == '_' || c == ':';
        if capture.as_str().len() <= 256
            && !text[..capture.start()]
                .chars()
                .next_back()
                .is_some_and(adjacent)
            && !text[capture.end()..].chars().next().is_some_and(adjacent)
        {
            found.insert(capture.as_str().to_owned());
        }
    }
}
fn inlines(values: &[Inline], found: &mut BTreeSet<String>) {
    for value in values {
        match value {
            Inline::Code(text) => symbols(text, found),
            Inline::Emphasis(inner)
            | Inline::Strong(inner)
            | Inline::Link { content: inner, .. } => inlines(inner, found),
            _ => {}
        }
    }
}
fn blocks(values: &[ContentBlock], found: &mut BTreeSet<String>) {
    for value in values {
        match value {
            ContentBlock::Paragraph(values) => inlines(values, found),
            ContentBlock::Code { text, .. } => symbols(text, found),
            ContentBlock::List { items, .. } => {
                for item in items {
                    blocks(item, found);
                }
            }
            ContentBlock::Table { rows, .. } => {
                for row in rows {
                    for cell in row {
                        inlines(&cell.content, found);
                    }
                }
            }
            ContentBlock::Note { blocks: values, .. } | ContentBlock::Quote(values) => {
                blocks(values, found)
            }
        }
    }
}

/// Exact qualified identifiers only; no inferred entities or semantic matching.
pub fn valid_entity(symbol: &str) -> bool {
    symbol.len() <= 256 && PATH.find(symbol).is_some_and(|m| m.as_str() == symbol)
}

pub(crate) fn derive(document: &ExtractedDocument, chunks: &[Chunk]) -> Result<DocumentGraph> {
    let mut links = Vec::new();
    let mut seen = BTreeSet::new();
    let mut truncated = false;
    for link in &document.links {
        let Some(url) = &link.url else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            continue;
        }
        if url.as_str().len() > 4096 {
            truncated = true;
            continue;
        }
        if !seen.insert(url.as_str()) {
            continue;
        }
        if links.len() == MAX_LINKS {
            truncated = true;
            break;
        }
        let mut target_url = url.clone();
        target_url.set_fragment(None);
        links.push(Link {
            target_url,
            url: url.clone(),
            text: link.text.chars().take(200).collect(),
        });
    }
    links.sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));
    let mut mentions = Vec::new();
    'chunks: for chunk in chunks {
        ensure!(
            chunk.source_id == document.page.source_id
                && chunk.document_url == document.canonical_url,
            "chunk graph provenance mismatch"
        );
        let section = document
            .sections
            .get(chunk.section_id)
            .context("invalid graph section")?;
        let mut found = BTreeSet::new();
        if let Some(heading) = &section.heading {
            symbols(heading, &mut found);
        }
        blocks(
            section
                .blocks
                .get(chunk.block_start..chunk.block_end)
                .context("invalid graph block range")?,
            &mut found,
        );
        for symbol in found {
            if mentions.len() == MAX_MENTIONS {
                truncated = true;
                break 'chunks;
            }
            mentions.push(Mention {
                symbol,
                chunk_id: chunk.id.clone(),
                section_id: chunk.section_id,
                url: chunk.source_url.clone(),
            });
        }
    }
    Ok(DocumentGraph {
        version: GRAPH_VERSION,
        links,
        mentions,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chunking::{ChunkPolicy, index_document},
        crawler::{PageOutcome, PageState},
        extraction::{ExtractionOutcome, extract},
    };
    use std::time::SystemTime;

    fn document(html: &str) -> Box<ExtractedDocument> {
        match extract(PageOutcome {
            source_id: "docs".into(),
            crawl_id: "test".into(),
            requested_url: "https://example.test/docs/".into(),
            final_url: "https://example.test/docs/".into(),
            fetched_at: SystemTime::now(),
            status: 200,
            headers: vec![],
            raw_body: html.as_bytes().to_vec(),
            content_truncated: false,
            rendering: None,
            state: PageState::Fetched,
        }) {
            ExtractionOutcome::Extracted(document) => document,
            _ => panic!("fixture must extract"),
        }
    }
    #[test]
    fn paths_are_case_sensitive_and_bounded() {
        let mut found = BTreeSet::new();
        symbols(
            "reqwest::Proxy::all() Widget::new éRust::Client Rust::Clienté x::::Y https://host",
            &mut found,
        );
        assert_eq!(
            found,
            BTreeSet::from(["reqwest::Proxy::all".into(), "Widget::new".into()])
        );
        assert!(valid_entity("reqwest::Proxy"));
        assert!(!valid_entity("Proxy"));
        assert!(!valid_entity("reqwest::Proxy(); THROW 'bad'"));
    }

    #[test]
    fn nested_code_and_headings_keep_exact_chunk_evidence() {
        let document = document(
            "<main><h1>Docs</h1><p>Prose::only</p><p><strong><code>inline::Code</code></strong></p><h2 id='api'>heading::API</h2><ul><li><code>list::Item</code></li></ul><blockquote><p><code>quote::Code</code></p></blockquote><aside class='note'><p><code>note::Code</code></p></aside><table><tr><td><code>table::Cell</code></td></tr></table><pre><code>block::Code();</code></pre></main>",
        );
        let index = index_document(&document, ChunkPolicy { target_chars: 50 }).unwrap();
        let graph = derive(&document, &index.chunks).unwrap();
        let names: BTreeSet<_> = graph.mentions.iter().map(|m| m.symbol.as_str()).collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "inline::Code",
                "heading::API",
                "list::Item",
                "quote::Code",
                "note::Code",
                "table::Cell",
                "block::Code"
            ])
        );
        for mention in &graph.mentions {
            let chunk = index
                .chunks
                .iter()
                .find(|c| c.id == mention.chunk_id)
                .unwrap();
            assert!(chunk.text.contains(&mention.symbol));
            assert_eq!(chunk.section_id, mention.section_id);
        }
    }

    #[test]
    fn graph_derivation_caps_records_and_reports_incomplete_indexing() {
        let html = format!(
            "<main><h1>Bounded</h1><pre><code>{}</code></pre>{}</main>",
            (0..MAX_MENTIONS + 1)
                .map(|n| format!("Type{n}::new();"))
                .collect::<String>(),
            (0..MAX_LINKS + 1)
                .map(|n| format!("<a href='page{n}'>Page</a>"))
                .collect::<String>()
        );
        let document = document(&html);
        let index = index_document(&document, ChunkPolicy::default()).unwrap();
        let graph = derive(&document, &index.chunks).unwrap();
        assert!(graph.truncated);
        assert_eq!(graph.links.len(), MAX_LINKS);
        assert_eq!(graph.mentions.len(), MAX_MENTIONS);
    }
}

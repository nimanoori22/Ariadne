use ariadne::{
    chunking::{ChunkPolicy, OversizedReason, content_hashes, index_document, sha256},
    crawler::{PageOutcome, PageState},
    extraction::{ContentBlock, ExtractedDocument, ExtractionOutcome, extract},
};
use std::{
    collections::HashSet,
    time::{Duration, UNIX_EPOCH},
};

fn document(html: &str) -> Box<ExtractedDocument> {
    match extract(PageOutcome {
        source_id: "docs".into(),
        crawl_id: "run-1".into(),
        requested_url: "https://example.test/docs/client".into(),
        final_url: "https://example.test/docs/client".into(),
        fetched_at: UNIX_EPOCH + Duration::from_secs(123),
        status: 200,
        headers: vec![],
        raw_body: html.as_bytes().to_vec(),
        content_truncated: false,
        state: PageState::Fetched,
    }) {
        ExtractionOutcome::Extracted(document) => document,
        _ => panic!("expected extracted fixture"),
    }
}

#[test]
fn sha256_matches_known_vector_and_hashes_ignore_fetch_metadata() {
    assert_eq!(
        sha256(b"hello world"),
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
    let mut doc = document("<main><h1>Client</h1><p>Content</p></main>");
    let original = content_hashes(&doc).unwrap();
    assert_eq!(original.raw_sha256, sha256(&doc.page.raw_body));
    doc.page.fetched_at += Duration::from_secs(100);
    doc.page.crawl_id = "another-crawl".into();
    doc.page
        .headers
        .push(("etag".into(), b"changed-header".to_vec()));
    assert_eq!(content_hashes(&doc).unwrap(), original);
}

#[test]
fn normalized_hash_ignores_boilerplate_and_preserves_code_semantics() {
    let first = document(
        "<title>Client</title><nav>Old menu</nav><main><h1>Client</h1><p>A  sentence.</p><pre><code>    indented()</code></pre></main>",
    );
    let second = document(
        "<title>Client</title><nav>New menu</nav><main><h1>Client</h1><p>A sentence.</p><pre><code>    indented()</code></pre></main>",
    );
    let third = document(
        "<title>Client</title><main><h1>Client</h1><p>A sentence.</p><pre><code>indented()</code></pre></main>",
    );
    let a = content_hashes(&first).unwrap();
    let b = content_hashes(&second).unwrap();
    assert_ne!(a.raw_sha256, b.raw_sha256);
    assert_eq!(a.normalized_sha256, b.normalized_sha256);
    assert_ne!(
        b.normalized_sha256,
        content_hashes(&third).unwrap().normalized_sha256
    );
    let mut changed_link = document("<main><h1>Client</h1><p><a href='one'>Link</a></p></main>");
    let before = content_hashes(&changed_link).unwrap();
    changed_link.links[0].url = Some("https://example.test/docs/two".parse().unwrap());
    assert_ne!(
        content_hashes(&changed_link).unwrap().normalized_sha256,
        before.normalized_sha256
    );
}

#[test]
fn fixture_chunks_preserve_all_blocks_hierarchy_anchors_and_provenance() {
    let doc = document(include_str!("fixtures/documentation.html"));
    let index = index_document(&doc, ChunkPolicy { target_chars: 200 }).unwrap();
    assert!(!index.chunks.is_empty());
    for section in &doc.sections {
        let selected = index
            .chunks
            .iter()
            .filter(|chunk| chunk.section_id == section.id)
            .collect::<Vec<_>>();
        let blocks = selected
            .iter()
            .flat_map(|chunk| chunk.block_start..chunk.block_end)
            .collect::<Vec<_>>();
        assert_eq!(blocks, (0..section.blocks.len()).collect::<Vec<_>>());
        for chunk in selected {
            assert_eq!(chunk.source_id, "docs");
            assert_eq!(chunk.document_url, doc.canonical_url);
            assert_eq!(chunk.source_url.fragment(), section.anchor.as_deref());
            assert_eq!(chunk.crawled_at, doc.page.fetched_at);
            assert_eq!(chunk.crawl_id, doc.page.crawl_id);
            assert_eq!(chunk.char_count, chunk.markdown.chars().count());
            assert!(chunk.token_count.is_none());
            assert_eq!(chunk.char_count > 200, chunk.oversized.is_some());
        }
    }
    let proxy = index
        .chunks
        .iter()
        .find(|chunk| chunk.heading_path.len() > 1)
        .unwrap();
    assert_eq!(proxy.heading_path[0], "Client");
    assert!(proxy.text.contains("Client"));
    assert_eq!(
        index
            .chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (0..index.chunks.len()).collect::<Vec<_>>()
    );
}

#[test]
fn identical_input_has_identical_index_and_recrawl_keeps_chunk_identity() {
    let mut doc = document(include_str!("fixtures/documentation.html"));
    let a = index_document(&doc, ChunkPolicy::default()).unwrap();
    assert_eq!(a, index_document(&doc, ChunkPolicy::default()).unwrap());
    doc.page.crawl_id = "run-2".into();
    doc.page.fetched_at += Duration::from_secs(1);
    let b = index_document(&doc, ChunkPolicy::default()).unwrap();
    assert_eq!(a.metadata, b.metadata);
    assert_eq!(
        a.chunks.iter().map(|chunk| &chunk.id).collect::<Vec<_>>(),
        b.chunks.iter().map(|chunk| &chunk.id).collect::<Vec<_>>()
    );
    assert_ne!(a.chunks[0].crawl_id, b.chunks[0].crawl_id);
}

#[test]
fn sections_split_at_block_boundaries_without_crossing_headings() {
    let doc = document(
        "<main><h1>Client</h1><p>First paragraph explains clients.</p><p>Second paragraph explains options.</p><p>Third paragraph explains defaults.</p><h2>Proxy</h2><p>Proxy configuration is separate.</p></main>",
    );
    let index = index_document(&doc, ChunkPolicy { target_chars: 65 }).unwrap();
    let client = index
        .chunks
        .iter()
        .filter(|chunk| chunk.heading_path == ["Client"])
        .collect::<Vec<_>>();
    assert_eq!(client.len(), 3);
    assert!(index.chunks.iter().all(|chunk| chunk.oversized.is_none()));
    assert!(
        client
            .iter()
            .all(|chunk| chunk.block_end - chunk.block_start == 1)
    );
    assert!(index.chunks.last().unwrap().heading_path == ["Client", "Proxy"]);
}

#[test]
fn explanation_and_code_remain_together_even_when_oversized() {
    let code =
        "    let proxy = Proxy::custom();\n    // ``` literal fence\n    configure(proxy);\n";
    let doc = document(&format!(
        "<main><h1>Proxy</h1><p>Configure a custom SOCKS proxy like this:</p><pre><code>{code}</code></pre><p>Next independent paragraph.</p></main>"
    ));
    let index = index_document(&doc, ChunkPolicy { target_chars: 65 }).unwrap();
    let example = &index.chunks[0];
    assert_eq!(example.oversized, Some(OversizedReason::ExampleGroup));
    assert_eq!((example.block_start, example.block_end), (0, 2));
    assert!(example.text.contains(code));
    assert!(example.text.contains("Configure a custom SOCKS proxy"));
    assert!(example.markdown.contains("````\n"));
    assert_eq!(index.chunks[1].block_start, 2);
}

#[test]
fn oversized_table_and_code_are_retained_intact_and_marked() {
    let cell = "table value ".repeat(20);
    let code = "    line();\n".repeat(30);
    let doc = document(&format!(
        "<main><h1>Reference</h1><table><tr><th>Meaning</th></tr><tr><td>{cell}</td></tr></table><pre><code>{code}</code></pre></main>"
    ));
    let index = index_document(&doc, ChunkPolicy { target_chars: 60 }).unwrap();
    assert_eq!(index.chunks.len(), 2);
    assert_eq!(index.chunks[0].oversized, Some(OversizedReason::Table));
    assert_eq!(index.chunks[1].oversized, Some(OversizedReason::Code));
    assert!(index.chunks[0].text.contains(cell.trim()));
    assert!(index.chunks[1].text.contains(&code));
    assert_eq!(index.metadata.oversized_chunk_count, 2);
}

#[test]
fn unicode_uses_characters_instead_of_bytes() {
    let doc = document(&format!(
        "<main><h1>参考</h1><p>{}</p><p>{}</p></main>",
        "配置代理".repeat(10),
        "默认设置".repeat(10)
    ));
    let index = index_document(&doc, ChunkPolicy { target_chars: 100 }).unwrap();
    assert_eq!(index.chunks.len(), 1);
    assert!(index.chunks[0].markdown.len() > 100);
    assert!(index.chunks[0].char_count < 100);
    assert!(index.chunks[0].oversized.is_none());
}

#[test]
fn repeated_identical_sections_and_blocks_have_unique_ids() {
    let doc = document(
        "<main><h1>Repeated</h1><p>Same content repeated.</p><p>Same content repeated.</p><h1>Repeated</h1><p>Same content repeated.</p></main>",
    );
    let index = index_document(&doc, ChunkPolicy { target_chars: 40 }).unwrap();
    assert_eq!(index.chunks.len(), 3);
    assert_eq!(
        index
            .chunks
            .iter()
            .map(|chunk| &chunk.id)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        index.chunks[0].content_sha256,
        index.chunks[1].content_sha256
    );
}

#[test]
fn unrelated_section_changes_do_not_change_existing_chunk_ids() {
    let first = document(
        "<main><h1 id='client'>Client</h1><p>Stable documentation.</p><h2 id='proxy'>Proxy</h2><p>Old documentation.</p></main>",
    );
    let second = document(
        "<main><h1 id='client'>Client</h1><p>Stable documentation.</p><h2 id='builder'>Builder</h2><p>New section.</p><h2 id='proxy'>Proxy</h2><p>New documentation.</p></main>",
    );
    let a = index_document(&first, ChunkPolicy::default()).unwrap();
    let b = index_document(&second, ChunkPolicy::default()).unwrap();
    assert_eq!(a.chunks[0].id, b.chunks[0].id);
    assert_ne!(
        a.chunks[1].content_sha256,
        b.chunks.last().unwrap().content_sha256
    );
    assert_ne!(
        a.metadata.hashes.normalized_sha256,
        b.metadata.hashes.normalized_sha256
    );
}

#[test]
fn heading_only_sections_remain_searchable_and_empty_intro_is_skipped() {
    let doc = document("<main><h1>Client</h1><h2>Builder</h2><p>Builder documentation.</p></main>");
    let index = index_document(&doc, ChunkPolicy::default()).unwrap();
    assert_eq!(index.chunks.len(), 2);
    assert_eq!(index.chunks[0].text, "Client");
    assert_eq!(
        (index.chunks[0].block_start, index.chunks[0].block_end),
        (0, 0)
    );
    assert!(index.chunks.iter().all(|chunk| chunk.section_id != 0));
}

#[test]
fn malformed_structure_and_invalid_policy_return_errors() {
    let mut doc = document("<main><h1>Client</h1><p>Documentation.</p></main>");
    assert!(index_document(&doc, ChunkPolicy { target_chars: 0 }).is_err());
    assert!(
        index_document(
            &doc,
            ChunkPolicy {
                target_chars: 1_000_001
            }
        )
        .is_err()
    );
    doc.sections[1].parent_id = Some(1);
    assert!(index_document(&doc, ChunkPolicy::default()).is_err());
    doc.sections[1].parent_id = None;
    doc.sections[1].blocks = vec![ContentBlock::Paragraph(vec![])];
    let index = index_document(&doc, ChunkPolicy { target_chars: 1 }).unwrap();
    assert_eq!(
        index.chunks[0].oversized,
        Some(OversizedReason::HeadingContext)
    );
}

# Structured HTML extraction

`ariadne::extraction::extract(page)` consumes a crawler page and returns either
an `ExtractedDocument` or an explicit rejection with the original page retained.
`ariadne::ingestion::extract_crawl(report)` processes a complete crawler report
and carries forward run identity, timestamps, blocked URLs, and delivery-loss flags.

```rust
use ariadne::{
    crawler::crawl,
    extraction::{ExtractionOutcome, ExtractionQuality},
    ingestion::extract_crawl,
};

// Inside an async function, after constructing a CrawlRequest:
let batch = extract_crawl(crawl(request).await?);
for outcome in batch.outcomes {
    match outcome {
        ExtractionOutcome::Extracted(document) => {
            // Inspect LowContent and diagnostics before deciding to index.
            let markdown = document.markdown();
            let text = document.plain_text();
            // Persist sections/blocks together with document.page provenance.
        }
        ExtractionOutcome::Rejected { page, reason } => {
            // Record the failure; raw content and fetch metadata remain intact.
        }
    }
}
```

## Document model

- The original `PageOutcome` retains raw bytes, requested/final URLs, source/run
  IDs, fetch timestamp, status, headers, and truncation metadata.
- `extraction_version` is `html-structure-v1`; algorithm changes can trigger
  reprocessing from the retained bytes later.
- Section IDs are ordered document-local indexes. Section zero is an unheaded
  introduction. Heading levels determine parent relationships, including skipped
  levels. IDs/anchors are retained when present; no anchors are invented.
- Blocks preserve paragraphs, code and language hints, nested ordered/unordered
  lists, table captions/cells/spans, recognized notes, and quotations.
- Inline nodes retain code, emphasis, strong text, line breaks, and links.
  Whitespace is normalized in prose while code block text remains exact.
- The effective fetched URL remains the document's canonical source URL without
  a fragment. A declared canonical link is retained separately as an untrusted
  hint for future deduplication decisions.
- Links resolve against the final URL or a valid HTML base URL. Original hrefs
  remain available, including unsupported link targets. Only HTTP(S) targets
  without embedded credentials become active links in generated Markdown.

## Selection and diagnostics

The extractor selects the first visible `main`/`role=main` container, otherwise
the first visible `article`, otherwise the body with a `BodyFallback` diagnostic.
Multiple candidate containers are reported; their content is not merged yet.

Known navigation, site headers/footers, sidebars, cookie controls, scripts,
templates, form controls, and hidden content are excluded. Documentation headers
inside the selected content are preserved. Recognized note/warning asides remain.
This is deterministic structural filtering, not a universal readability algorithm.

Empty useful content is rejected. Short output (fewer than 40 non-whitespace
characters) is retained with `LowContent` and `SparseContent`; this is a heuristic,
not a correctness score or an automatic browser-rendering decision. Missing titles
fall back to the first H1, then the source URL with an explicit diagnostic.

Failed/truncated fetches, non-HTML content types, explicitly unsupported HTTP
charsets, invalid UTF-8, invalid source URLs, input over 16 MiB, and excessive
traversal depth are rejected with their raw page retained. Missing content-type
headers allow HTML parsing; legacy encodings require a future decoding stage.

Markdown and plain text are derived from the structured model. Code fences grow
when the code contains backticks, table pipes are escaped, and relative links
are resolved. Markdown tables cannot express every cell-span layout; the structured
cells retain those semantics for storage and retrieval.

## Reuse and reference evaluation

Production uses Spider's `spider_scraper` DOM parser (pinned 0.1.2, imported as
`scraper`). This avoids implementing HTML parsing or adopting another crawler.

The pinned `spider_transformations` 2.39.13 converter is a development-only
comparison dependency. Against the shared documentation fixture, its default
`transform_markdown(html, false)` shortcut preserves API text and removes standard
navigation, but does not preserve the fixture's exact code indentation or resolve
its relative links using the declared HTML base URL. Those observations apply to
this shortcut/configuration and fixture, not every transformation API.

Ariadne therefore keeps a DOM-derived section/block model and renders from that
model. Its serializer serves hierarchy/provenance needs that a flat Markdown
string cannot represent. The conversion baseline remains tested for future review.

Source-level references included Crawl4AI's link/base-URL processing and Markdown
pipeline, and Spider's agent HTML cleaner. The cleaner's aggressive attribute
removal would lose hrefs, so it is not used before documentation extraction.

## Validation and next work

Fixtures test hierarchy/anchors, exact code, inline spacing, nested lists, tables,
hidden table groups, warnings, links/base URLs, provenance, canonical hints,
body/article selection, sparse/empty results, malformed HTML, encoding failures,
unsafe link rendering, and literal untrusted source text. A local crawl-to-extraction
test verifies the pipeline and preservation of failed/incomplete run outcomes.

Pending: site-specific selectors, multiple-root merging, browser quality criteria,
encoding detection/decoding, richer definition-list/media structures, nested-table
layout, and configurable extraction resource limits. There is no chunking,
SurrealDB persistence, embedding generation, retrieval, or MCP in this stage.

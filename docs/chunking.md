# Semantic chunking and content identity

The ingestion path is now:

```text
Spider → structured extraction → hashes + semantic chunks
       → atomic SurrealDB document/section/chunk replacement
```

`chunking::index_document` is pure synchronous processing. It accepts an extracted
document and a `ChunkPolicy`, returning a `DocumentIndex`. Ingestion's
`prepare_crawl` handles a batch, preserving every rejection and crawl audit.
The prepared indexes are private so callers cannot mismatch derived records with
their source documents. `KnowledgeStore::finish_prepared_crawl` persists that
result; the original `finish_crawl` convenience method prepares with defaults.

## Hashes and normalization

All hashes are SHA-256 lowercase hex. The direct `sha2` dependency is already used
transitively by the database stack; the standard library does not provide SHA-256.

- `raw_sha256`: exact retained response bytes.
- `normalized_sha256`: deterministic JSON serialization of normalization version,
  document title, ordered sections/blocks, and links. Heading hierarchy, anchors,
  table structure, inline semantics, resolved links, and exact code whitespace
  matter. Fetch times, crawl IDs, HTTP headers, raw boilerplate, diagnostics, and
  URL identity outside the structured content do not participate.
- `normalization_version`: `structured-document-v1`. Normalization uses the
  existing extractor's structured model, not Spider's HTML fingerprint or a
  Markdown-to-text round trip. Prose whitespace has already been normalized by
  extraction; code whitespace remains intact.

Processing metadata also records extraction version, chunking version, policy,
chunk count, and oversized chunk count. Schema additions are applied by bundled
SurrealKit sync. Older documents survive the upgrade and report no indexing
metadata/chunks until successfully processed again; there is no automatic
unbounded startup backfill.

Spider's indexed `hash_html → normalize_html` path was inspected. Its normalization
removes link hrefs and most attributes, so it is not the semantic identity used
here. Indexed Crawl4AI fixed-length and overlapping word strategies split/join
whitespace; these are useful references but do not preserve this project's
structured code and section boundaries. The Spider file had partial graph coverage;
its reported missed ranges and the relevant normalization source were read directly.

## Chunk policy

Default policy: a **soft 2,400 Unicode character budget** measured in rendered
Markdown, including ancestor heading context. This is not a model token limit.
`token_count` stays `None` until the embedding provider and tokenizer are selected.
Embedding code must measure its actual input and enforce that provider's limits.

Chunks never cross sections. Sections are packed at ordered block boundaries:

- A paragraph immediately before one or more code blocks stays with those examples.
- Adjacent code blocks stay together.
- Code, tables, lists, notes, quotes, and individual paragraphs are atomic.
- Large sections split between these semantic units. No block is discarded or
  split through code, a table row, an inline construct, or a nested list.
- A heading-only section gets a chunk. An empty synthetic introduction does not.

A unit larger than the target is retained whole and explicitly marked with an
`OversizedReason` (code, table, example group, paragraph, list, note, quote, or
heading context). Oversized content is available for lexical retrieval and later
context assembly. It must not be sent blindly to an embedding model. Splitting
oversized paragraphs, table rows, and code examples with model-aware policies is
future work, not an implicit truncation policy.

Both plain text and Markdown carry the heading path. Markdown comes from the
existing structured renderer and retains safe fences, lists, tables, and links.
Packing uses prefix sums to avoid repeatedly rendering a growing section.

## Identity and provenance

Each chunk retains source ID, canonical document URL, document title, section ID,
heading path, anchor, URL with anchor, crawl ID/timestamp, global sequence, and a
half-open source block range. The original structured blocks remain in sections.

`content_sha256` hashes the versioned heading path and source block structure.
Chunk `id` additionally includes source, canonical URL, section locator
(anchor/heading path), policy, and duplicate occurrence numbers. Crawl timestamps,
global sequence, and numerical section IDs do not enter identity. This keeps
unchanged units stable across recrawls and unrelated section insertions, while
identical repeated sections/blocks still get distinct IDs. Adding an earlier
identical section can change occurrence-based identities; anchor and heading
changes intentionally change identity. These IDs are deterministic, not persistent
editor-assigned IDs.

Identical normalized representations of the same fetched canonical URL share one
document, even if their raw boilerplate differs; all fetch aliases remain audited.
Conflicting structured representations fail the batch. Different URLs or sources
retain separate documents and chunk IDs even when their content hashes match.
This preserves provenance and avoids silently merging versioned documentation.

## Persistence and inspection

Chunks are actual SurrealDB records with document and section record references.
Document indexing metadata lives alongside document metadata. Replacing a document
deletes obsolete chunks/sections and inserts the new complete set in the same
transaction as page audit and run completion. A rejected extraction or a failed
transaction preserves the previous document, chunks, and hashes.

Repeating successful ingestion produces the same IDs and record counts while
updating current fetch provenance. It still performs extraction, preparation,
and replacement; hashes alone do not implement conditional HTTP fetching or skip
unchanged processing. Those optimizations remain milestone 2. Crawl summaries now
include persisted chunk and oversized chunk counts.

```sh
cargo run -- chunks reqwest https://docs.rs/reqwest/latest/reqwest/
cargo run -- indexing reqwest https://docs.rs/reqwest/latest/reqwest/
```

Library callers can select a different policy:

```rust,ignore
let prepared = ariadne::ingestion::prepare_crawl(
    extracted_batch,
    ariadne::chunking::ChunkPolicy { target_chars: 1600 },
)?;
store.finish_prepared_crawl(prepared).await?;
```

Lexical retrieval and automatic full-text indexing are now available; see
`retrieval.md`. Model-scoped embeddings and vector retrieval are also implemented;
see `embeddings.md` for model limits and failure handling. MCP tools are next.

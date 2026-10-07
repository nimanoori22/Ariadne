# Ariadne implementation roadmap

Based on `AGENTS.md` and the repository inspected on 2026-10-07.

## Starting point

Progress update: the Spider adapter and structured HTML extraction are implemented
and fixture-tested. Storage now uses an application-owned embedded
SurrealDB/SurrealKV datastore and bundled SurrealKit schema sync; see
`docs/storage.md`. Source registration, crawl audit, document/section persistence,
and CLI inspection now have implementations. Versioned raw/structured hashes and
semantic chunking are also implemented and persisted atomically; see
`docs/chunking.md`. The initial chunk policy uses a character budget and marks
oversized atomic units. Embedding inputs are never silently truncated; provider
context rejection is recorded explicitly, while per-chunk token counts remain unknown.
Lexical retrieval is implemented with full-text indexes, precise identifier checks,
source filters, bounded source-backed hits, and CLI search; see `docs/retrieval.md`.
Embedding generation and vector retrieval are implemented with local Ollama,
model-scoped indexes, resumable state, reuse/invalidation, source filtering and
CLI operations; see `docs/embeddings.md`. A real embeddinggemma semantic fixture
has passed. Foreground ingestion orchestration and stdio MCP lexical/vector
search are implemented; the milestone 1 acceptance gate now passes, including
real local Ollama retrieval after restart and a bounded live documentation crawl.
See `docs/mcp.md`. Incremental recrawling is implemented with durable HTTP
validators, hash/version-aware reuse, atomic replacement, conservative removal,
offline reprocessing and CLI status; see `docs/recrawling.md`. The next work is
step 11: hybrid ranking and context expansion.

At the initial roadmap inspection, the repository contained a Rust 2024 package,
an empty dependency list, and a `src/main.rs` that printed “Hello, world!”. This
was the baseline for the work below. Build one vertical slice before expanding.

The first release must demonstrate:

```text
Register source → scoped static crawl → structured extraction → semantic chunks
→ SurrealDB → embeddings → lexical/vector retrieval → MCP search with provenance
```

Use one Rust package with modules initially. Keep persistence queries inside
`storage`, third-party crawl types inside the crawler adapter, and transport
types inside `mcp`/`cli`. Introduce traits only at boundaries that need substitution
or deterministic testing. Split crates when actual dependency boundaries justify it.

## Milestone 1: source-backed search through MCP

### 1. Validate integrations and record decisions

- Inspect indexed Spider configuration, crawl execution, page delivery, scope,
  robots, retries, response metadata, and feature flags. Prefer a thin Spider
  adapter if it meets the requirements.
- Inspect Crawl4AI extraction, content filtering, links, and Markdown generation
  as source-level references for a Rust structured extractor.
- Run a small Spider integration experiment against a local fixture server.
  Check whether page delivery can lose pages when ingestion is slower than crawling.
  The inspected `subscribe` implementation uses a Tokio broadcast channel;
  lag must become an explicit error or be prevented by a verified delivery strategy.
- Verify the chosen embedded SurrealDB SDK/engine pair with executable experiments for
  transactions, record references, full-text analyzers, vector dimensions,
  indexes, and filtered nearest-neighbor queries.
- Verify the MCP library's structured results and stdio transport support.
- Record compatible versions, selected features, tradeoffs, and a minimal local
  setup. Do not select an embedding provider solely from the conceptual examples.

**Done when:** a fixture page can be fetched through Spider, a representative
record can be stored and searched in SurrealDB, and a minimal MCP tool can be
called by a client. These are feasibility experiments, not full implementations.

### 2. Establish application boundaries and deterministic fixtures

- Add `domain`, `crawler`, `extraction`, `chunking`, `ingestion`, `storage`,
  `embeddings`, `retrieval`, `mcp`, and `cli` modules as they acquire real code.
- Define source, crawl run, page outcome, fetched page, structured document,
  section, chunk, embedding metadata, retrieval query, and knowledge hit types.
- Represent content with ordered blocks: paragraphs, code, lists, tables, notes,
  and links. Preserve section parents, heading levels, ordering, and anchors.
- Include requested/final/canonical URLs, timestamps, hashes, and raw response
  information needed to trace or reprocess a page.
- Add configuration validation, typed errors, structured tracing, and explicit
  cancellation. Keep credentials outside persisted crawl configuration.
- Build a local fixture website with nested headings, API signatures, code,
  tables, duplicate links, redirects, navigation, failures, and scope escapes.
- Add formatting, linting, unit tests, and isolated database integration checks.

**Done when:** representative extracted documents and retrieval hits can be
serialized with complete provenance, and fixture tests run without public websites.

### 3. Register sources and persist auditable crawl runs

- Add versioned database schema/setup scripts and a narrow knowledge-store boundary.
- Run SurrealDB embedded with SurrealKV by default. Create the application data
  directory automatically and apply compiled-in schema through SurrealKit's
  library API, without a separate server or migration CLI. Use explicit rollouts
  for destructive schema changes and data backfills.
- Store sources, crawl runs, page outcomes, documents, sections, and chunks;
  add embedding records in step 8. Use record references initially, adding graph
  edges where they answer an actual navigation or retrieval need.
- Enforce document identity within a source and deterministic derived identifiers.
- Store crawl scope, configuration, start/end times, status, counters, and failures.
- Add CLI operations for source registration/listing and crawl-run inspection.
- Define an atomic document replacement operation so readers never see a mixture
  of old and new sections/chunks. Failed processing must preserve the last good data.

**Done when:** sources and runs survive restart, duplicate registration has defined
behavior, and an interrupted or failed run remains inspectable.

### 4. Implement scoped static crawling through the adapter

- Translate application scope and budgets to verified Spider controls: allowed
  host/path, depth, page count, concurrency, timeouts, and response size limits.
- Validate discovered URLs and redirect destinations against scope before fetching.
- Normalize URLs conservatively: remove fragments for fetch identity, resolve
  relative links, and only remove query parameters known to be irrelevant.
- Keep frontier/visited management in the crawler engine where possible. Expose
  enough queue and page outcomes to audit its decisions without creating a second frontier.
- Enable bounded requests, robots handling, rate limits, bounded retries, and
  cancellation through verified engine capabilities; document any adapter additions.
- Deliver raw content and HTTP metadata to ingestion and persist each page outcome.

**Done when:** a local crawl respects every budget and scope boundary, reports
failures, and accounts for every fetched page even with slow downstream processing.

### 5. Extract structured documentation

- Prefer document main/article containers, with explicit fallback behavior.
- Remove navigation and repeated controls while preserving useful content.
- Build a heading tree and ordered content blocks directly from HTML/DOM.
- Preserve code whitespace/language, inline code, API signatures, tables, lists,
  warnings, anchors, and resolved links.
- Generate Markdown and plain text from the structured model.
- Retain raw content, extraction version, canonical URL decisions, and diagnostics.

**Done when:** fixture snapshots retain hierarchy and code exactly, remove known
boilerplate, and report empty or low-quality extraction as an explicit outcome.

### 6. Normalize, hash, chunk, and persist documents

- Define versioned normalization and stable raw/normalized content hashes.
- Chunk at section and block boundaries; keep examples with their explanation.
- Subdivide oversized sections deterministically. Define an explicit oversized
  code/table policy instead of silently dropping content or exceeding model limits.
- Retain section/document IDs, sequence, heading path, source location, and hash.
- Keep token accounting compatible with the selected embedding model's limits.
- Persist a complete document and its derived records atomically; make repeat
  ingestion idempotent and remove obsolete derived records safely.
- Deduplicate fetch aliases and identical content while retaining source locations.

**Done when:** identical input produces identical chunks, repeated ingestion creates
no duplicate records, and failed replacement preserves the previous document.

### 7. Deliver lexical retrieval first

- Configure SurrealDB full-text indexing for documentation content.
- Verify punctuation-sensitive identifiers such as `Proxy::custom` and
  `ClientBuilder`; add an exact identifier path only if analyzer behavior requires it.
- Return bounded, deterministically ordered knowledge hits with text, source ID,
  URL/anchor, document title, section path, crawl timestamp, chunk ID, and score.
- Exercise retrieval through the CLI using a small relevance fixture.

**Done when:** exact API queries return expected sections with traceable sources,
and searching works independently of embedding availability.

### 8. Generate embeddings and deliver vector retrieval

**Implemented:** Ollama provider and deterministic test double, bounded batches,
checkpointed failures, model/revision/dimension isolation, SurrealKit-managed
HNSW indexes, exact source-filtered cosine search, and source-backed CLI retrieval.
Real-model semantic smoke verified; see `docs/embeddings.md` for limits and policy.

- Add a provider boundary and one real provider, plus a deterministic test double.
- Batch within provider limits, bound concurrency/retries, and checkpoint failures.
- Store provider/model identity, dimensions, creation time, and chunk content hash.
- Reuse embeddings only when content and model identity match; track pending,
  failed, and stale embeddings explicitly.
- Configure the vector index for the selected dimension/distance settings and
  implement query embedding plus nearest-neighbor retrieval.
- Define model migration and partial indexing behavior. Do not compare incompatible
  vector spaces or replace valid embeddings with failed outputs.

**Done when:** a real semantic query retrieves relevant fixture content; dimension
errors, stale vectors, and provider failures have tested behavior.

### 9. Complete ingestion orchestration and MCP search

**Implemented:** auditable `ingest`, provider failure isolation and interruption
recovery, official Rust MCP SDK stdio server, bounded lexical/vector `search`,
structured provenance, stderr tracing and documented client configuration.
Local HTTP ingestion → embedded persistence → fresh MCP process acceptance
passes with both deterministic and real Ollama embeddings. A one-page live
docs.rs crawl stored one document and 76 chunks and retrieved `Proxy::all`
with its section anchor. See `docs/mcp.md`.

- Connect source/run creation, crawling, extraction, chunking, persistence, and
  embedding generation with bounded work and explicit stage failures.
- Expose one `search` MCP tool backed by the retrieval service. Include query,
  bounded limit, and a defined lexical/vector mode; leave hybrid ranking to milestone 2.
- Return structured hits and distinguish retrieved source content from instructions.
- Keep logs off the stdio protocol stream and test with a real MCP client.
- Document a reproducible local command sequence and source registration/crawl usage.

**Milestone acceptance:** crawl a small local documentation site into SurrealDB,
restart the application, and retrieve correct results through MCP in lexical and
vector modes. Every result includes provenance; every crawl failure is visible.
Add one deliberately bounded real documentation crawl as a manual smoke test.

## Milestone 2: reliable recrawling and richer retrieval

### 10. Add incremental recrawling

**Implemented:** persisted ETag/Last-Modified validation through Spider's fetch
hook; 304/raw/structured reuse; processing-version and policy invalidation;
atomic chunk/vector replacement; direct 404/410 removal with retained raw content;
no absence pruning; offline reprocessing; CLI recrawl/status and interruption
recovery. Deterministic fixtures verify unchanged runs generate no embeddings and
single-page changes update only affected data. See `docs/recrawling.md`.

- Persist ETag/Last-Modified validators and correctly handle conditional responses.
- Skip unchanged normalization, chunking, and embedding work using content hashes
  plus processing/model versions. Allow reprocessing retained raw content.
- Replace changed documents and invalidate obsolete chunks/vectors atomically.
- Track crawl completeness before considering pages absent: a failed or budgeted
  crawl must not remove documents just because it did not revisit them.
- Add recrawl/status operations and tests for unchanged, changed, removed, and
  interrupted runs.

**Done when:** an unchanged recrawl generates no new embeddings, a one-page change
updates only affected data, and incomplete discovery cannot delete good knowledge.

### 11. Add hybrid ranking, source filters, and context expansion

- Combine lexical/vector candidates through a replaceable ranking implementation.
  Evaluate rank fusion before combining scores with incompatible scales.
- Add source and metadata filters, verifying filtered vector-search recall.
- Expand hits to parent headings, neighboring blocks, and associated examples
  under a response budget; deduplicate overlapping context.
- Maintain a query relevance set covering exact identifiers and conceptual questions.

**Done when:** evaluation demonstrates a useful improvement over individual search
modes and expanded context remains bounded and source-backed.

### 12. Expand agent access and operational reliability

- Add `get_document`, `get_section`, `list_sources`, `source_status`, `crawl`,
  and `recrawl` tools as backed use cases become available.
- Return job IDs promptly for crawl operations; expose progress and cancellation.
- Add structured discovery through sitemap/navigation and llms files where useful,
  preserving original page provenance and applying the same scope rules.
- Add restart recovery, interrupted-job handling, database migration checks,
  backup/restore guidance, and resource/latency measurements.
- Test arbitrary URL inputs and redirects so remotely callable crawling has explicit
  network access boundaries; configure access control before remote deployment.

**Milestone acceptance:** two independently scoped sources can be recrawled and
searched through MCP with filters, hybrid ranking, context, and inspectable status.

## Later work, driven by demonstrated retrieval needs

1. Browser fallback through Spider for sources whose static responses lack content.
2. Useful document-link graph traversal and deterministic entity extraction.
3. Graph-assisted retrieval, measured against the query relevance set.
4. Document version history and version-aware retrieval.
5. Reranking, optional LLM-assisted extraction, and task-directed crawling.
6. Research workflows, cross-source relationships, and agent memory.

Defer distributed crawling, broad internet discovery, extra databases, many
embedding providers, and sentence-level LLM graph generation.

## Working rule for each implementation step

Inspect applicable reference source, implement the smallest necessary addition,
test deterministic behavior and affected integration boundaries, then run formatting,
linting, and tests. Record decisions and limitations as they become concrete.
Proceed to the next milestone only when the current acceptance gate passes.

# Incremental recrawling

Step 10 adds durable conditional HTTP validation, processing reuse, offline
reprocessing and source/document status. Spider remains the crawler engine;
Ariadne adds a persisted-page fetch hook and knowledge replacement decisions.

## Commands

After registering and ingesting a source, use a new crawl ID on every run:

```sh
./target/debug/Ariadne recrawl reqwest refresh-001 --max-pages 100
./target/debug/Ariadne source status reqwest
./target/debug/Ariadne document-status reqwest https://docs.rs/reqwest/0.13.5/reqwest/struct.Proxy.html
./target/debug/Ariadne run reqwest refresh-001
```

`recrawl` uses local Ollama by default, with the same model configuration as
`ingest`. Pass `--lexical-only` to update searchable text without Ollama.
`--concurrency` bounds requests; `--chunk-chars` changes the semantic chunk budget.
Scope, timeouts, response-size limits, robots handling and redirect validation
remain in force. The CLI defaults to 100 pages and two concurrent requests.

Reprocess retained HTML without network requests or an embedding provider:

```sh
./target/debug/Ariadne reprocess reqwest local-001 --max-pages 100 --chunk-chars 2000
./target/debug/Ariadne embed reqwest
```

`reprocess` deliberately rebuilds active documents from retained raw bodies.
The separate `embed` command fills missing derivatives afterward. Offline
processing preserves the previous HTTP validation timestamp and headers; it
records a new processing run without claiming that the website was checked.
Existing removed documents are excluded from offline processing.

## Decisions and provenance

For a previously indexed, intact HTML representation at the same requested and
final URL, the fetch hook sends valid `If-None-Match` and/or `If-Modified-Since`
headers. Invalid validators, retained-body hash mismatches, `Cache-Control:
no-store` and nonempty `Vary` disable conditional reuse. Redirected representations
are handled conservatively; a redirected 304 triggers an unconditional request.
These are retained knowledge documents rather than a general HTTP cache.
Conditional semantics follow [HTTP RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-13.1.2).

- **304, compatible processing:** reuse sections, chunks and embeddings. Retained
  HTML still supplies Spider with links for discovery; audit records retain the
  actual 304 and an empty response body.
- **200, identical raw hash:** skip extraction, normalization and chunking.
- **200, different bytes but identical structured hash:** retain the newly fetched
  raw/structured document while preserving existing chunks and embeddings.
- **Changed structured content:** replace that document's sections/chunks and
  invalidate obsolete vectors in one database transaction.
- **Different extraction, normalization or chunking version, or chunk policy:**
  reprocess even when the server returns 304. A validator validates bytes, not
  the algorithm used to turn them into knowledge.

Embedding reuse separately checks model/revision/dimensions and chunk content.
An unchanged recrawl with a compatible, fully indexed embedding space generates
zero vectors. Changing the model can generate vectors for unchanged documents
in a separate space. Pending or failed embeddings are retried through the
existing source-wide embedding pipeline.

`document-status` separates current availability and `last_checked_at` from
content-fetch provenance. An unchanged chunk keeps its original crawl ID and
content timestamp; the later validation run is visible in status and audit.
`source status` returns active/removed document counts and the latest run.
`run` includes incremental counts for added, changed, reprocessed, unchanged,
removed, known documents and unvisited known documents. `chunk_count` reports
chunks produced in that run, rather than total chunks in the source.

## Missing pages and failures

A directly observed 404 or 410 at a known document's own URL marks it removed:
its retrieval chunks disappear and vectors become stale, while raw content and
sections remain available for inspection. A later successful fetch restores it.
Redirect failures, 5xx responses, oversized bodies and extraction rejection
preserve the last good knowledge.

Absence from discovery never implies deletion. Previously stored URLs are
seeded alongside the root for multi-page recrawls, subject to scope/depth/budgets.
Single-page recrawls check the root only. The additional persisted seed list is
capped at 10,000 URLs; bodies are loaded on demand. Run metadata explicitly
reports that complete coverage is not proven and absence pruning is disabled.
Unvisited known documents, rejected pages or audit overflow make ingestion
partial; the CLI prints the run and exits unsuccessfully. This conservative
policy avoids deleting knowledge after a budgeted or failed crawl.

A source accepts only one active crawl preparation/persistence run. Cancellation
before the atomic commit leaves previous knowledge intact. On reopening the
exclusive embedded store, abandoned runs are marked interrupted. Embedding
failures after commit retain lexical updates and report partial ingestion.
Explicit rolled-back transaction write conflicts are retried up to five times
with bounded backoff, including conflicts with background vector maintenance.
Other database errors are surfaced without retrying.

The deterministic fixture suite checks conditional reuse after restart,
validator rotation, hash-based reuse, processing-version/policy changes, model
isolation, changes/removals/restoration, scope and body limits, overflow,
interruption, CLI status and offline processing:

```sh
cargo test --test recrawling
```

Recrawl/status are CLI and library operations in this step. MCP still exposes
search; additional agent operations remain step 12. Hybrid ranking and context
expansion are the next implementation step.

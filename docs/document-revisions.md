# Document revisions and historical retrieval

A successful ingestion now stores an immutable indexed representation alongside
its live document, sections and chunks. When a recrawl changes content, both the
old and new representations remain readable after restart. Default lexical,
vector, hybrid and graph searches still use only live chunks. A confirmed
404/410 removes live retrieval units while preserving revision history.

## Identity and provenance

Each revision has a 64-character lowercase SHA-256 ID scoped to the source and
canonical URL. Its identity includes the normalized content hash, extraction,
normalization and chunking versions, and chunk character policy. Crawl IDs,
timestamps, HTTP headers and boilerplate-only changes are excluded. The pinned
SurrealDB engine hashes an ordered representation of these values inside the
commit transaction. Changing processing versions/policy creates a new indexed
representation even when normalized content is unchanged.

The archive retains the first observation of that representation: raw HTML and
response metadata, structured sections/links, indexing hashes/policy, complete
chunks, crawl ID and content fetch time. `stored_at` records when it was first
archived; it can be later than the content fetch time when backfilling an old
store. Revision snapshots are never refreshed by unchanged recrawls. Raw HTML
is available through the explicit library inspection API `get_revision`;
bounded agent reads return headings and chunks, with source data labels.

A → B → A reuses the original A revision and moves the current pointer back to
it. The per-document sequence orders distinct representations by first archival,
not every visit. Crawl audits remain the record of visits, failures, validators
and reversions. `is_current` means the latest retained representation; use
`document-status`/`source_status` to distinguish active and removed documents.

Snapshotting, chunk replacement, graph updates, vector invalidation, audit writes
and crawl completion share one transaction. A failure rolls them all back.
Separate revision section/chunk tables prevent historical records from entering
current full-text, vector or graph candidate sets. No vectors or graph edges are
archived in this milestone.

## CLI

```sh
Ariadne revisions docs https://example.test/docs/client --limit 20
# Use the numeric next_cursor as --after to read another page.
Ariadne revisions docs https://example.test/docs/client --after 20 --limit 20
Ariadne revision docs https://example.test/docs/client REVISION_ID
# Optional last argument selects one historical section by its numeric ID.
Ariadne revision docs https://example.test/docs/client REVISION_ID 2
Ariadne revision-search docs https://example.test/docs/client REVISION_ID 'Old::configure'
Ariadne revision-retrieve docs https://example.test/docs/client REVISION_ID 'Old::configure' --neighbors 1 --context-chars 1000
```

`revisions` returns bounded metadata pages (1–100, default 20), hashes,
processing versions, chunk/section counts, provenance and the current pointer.
`revision` uses a 4,000-character total text budget and at most 100 headings and
chunks, with explicit truncation. `revision-search` accepts existing lexical
search options/filters. `revision-retrieve` defaults to lexical mode and accepts
existing context budgets; vector/hybrid modes are rejected. HTTP URL fragments
are removed for document identity. These commands never fetch a URL or contact
Ollama.

## MCP

The server now exposes twelve tools. `list_revisions` takes `source_id`, `url`,
optional `after` and `limit`. Existing `get_document` and `get_section` accept
optional `revision_id`; omit it to read current data. Historical section IDs
belong to their revision and may differ from the current document.

```json
{
  "query": "Old::configure",
  "mode": "lexical",
  "revision": {
    "source_id": "docs",
    "document_url": "https://example.test/docs/client",
    "revision_id": "64-lowercase-hex-characters-from-list_revisions"
  },
  "context": {"neighbor_chunks": 1, "max_total_chars": 1000}
}
```

The example ID is a placeholder; use the actual SHA-256 returned by
`list_revisions`. Explicit historical search supports keyword/exact lexical
matching and the same metadata filters, applied before limits. Hits and expanded
context include `revision_id`, original chunk IDs/hashes, crawl timestamps,
source URLs and section headings. Context expansion stays within that revision;
identical chunk IDs from separate revisions are not merged. A missing or
out-of-scope revision returns no search hits; document reads report not found.
An invalid ID or conflicting source filter is rejected. Historical vector,
hybrid and graph requests are rejected before provider initialization.

Historical BM25 scores come from the archive's lexical indexes; they are not
comparable with current-index scores or probabilities. Source names come from
the current registered source; document content/provenance comes from the
snapshot. Omit `revision` to keep the existing current-search interface.

## Upgrade and limits

The additive schema is applied automatically with existing schema synchronization.
An existing document is archived on its next successful ingestion, recrawl or
reprocess, before replacement, refresh or confirmed removal. Opening the store
alone does not backfill it. Earlier documents without chunk metadata are preserved
as raw/structured legacy snapshots with a separate raw-content identity and no
historical search units. Reprocess them to create a current indexed revision.
Content replaced before this feature existed cannot be recovered from history.

History grows with distinct indexed representations. Retention/pruning, diffs,
named release versions, date-based selection, historical embeddings and historical
graph traversal remain later work. Closed-store backups include all revision
records; see [storage](storage.md). This is application-owned knowledge history
and does not enable SurrealKV temporal versioning.

## Validation and references

Deterministic tests cover A → B → restart with complete raw/structured content,
latest-only search, historical lexical/context reads, A → B → A, unchanged
recrawls, policy changes, Unicode budgets, pagination, source/URL isolation,
legacy backfill and transactional rollback. Fixture HTTP tests cover changed,
304, raw-identical, boilerplate-only and removed pages. Fresh CLI and stdio MCP
processes list, read and search revisions without Ollama.

Crawl4AI's indexed `AsyncDatabaseManager.acache_url` stores content hashes and
updates a URL cache row on conflict. Spider's `Website::get_pages` exposes retained
crawl pages. These inspected paths inform reuse of the existing fetch/cache
boundary; the revision archive belongs to our knowledge store. SurrealDB's
[hash functions](https://surrealdb.com/docs/reference/query-language/functions/database-functions/crypto),
[type conversion](https://surrealdb.com/docs/reference/query-language/functions/database-functions/type)
and [transactions](https://surrealdb.com/docs/reference/query-language/language-primitives/transactions)
provide the database primitives. Executable tests verify them against the pinned
engine rather than depending on native time travel.

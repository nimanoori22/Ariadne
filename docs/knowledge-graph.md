# Document links and deterministic entities

Ariadne now persists a small, source-backed knowledge graph alongside documents,
sections and chunks. These operations work without Ollama or network access.
Graph-assisted search ranking remains the next roadmap item.

## CLI and MCP

```sh
./target/debug/Ariadne links reqwest 'https://docs.rs/reqwest/latest/reqwest/struct.Proxy.html' incoming
./target/debug/Ariadne links reqwest 'https://docs.rs/reqwest/latest/reqwest/struct.Proxy.html' outgoing
./target/debug/Ariadne entity reqwest 'reqwest::Proxy'
```

CLI reads return at most 20 records. MCP exposes two read-only tools, with a
configurable `limit` of 1–100 (default 20):

```json
{"source_id":"reqwest","url":"https://docs.rs/reqwest/latest/reqwest/struct.Proxy.html","incoming":true,"limit":20}
```

Pass these arguments to `get_links`; omit `incoming` or set it to false for
outgoing links. `find_entity` accepts:

```json
{"source_id":"reqwest","entity":"reqwest::Proxy","limit":20}
```

Results are deterministic within a read transaction. All source text is labeled
`source_data`. Link evidence includes the originating source/document/title,
original resolved target URL with fragment, fragment-free target identity,
content crawl ID/time, graph policy version and target indexing/availability.
Entity evidence includes source/document/title, section ID, chunk ID, heading
path, section URL, content crawl ID/time and chunk hash. Use `get_section` or
`get_document` to read the surrounding source content.

`truncated` reports whether more results exist than the requested limit. Both
operations return source-level `coverage`: active document count, count indexed
with the current graph policy, and count whose graph derivation was truncated.
For sources with no active documents, coverage is null. `get_links` also returns
the queried document's availability and graph metadata, or null if it does not
exist. Missing graph coverage is distinct from evidence that no links/mentions
exist. Titles and link labels are capped at 200 characters; heading paths retain
at most 16 headings of 200 characters each. Title and heading truncation flags
are explicit. Full document/section content remains stored.

## Link identity and scope

`links_to` is a SurrealDB document-to-document relation. Targets retain the
source of their originating document: these reads never cross into another
registered source. Edges use canonical document identity and preserve queries;
fragments are removed only for target document lookup and retained in evidence.
Repeated identical resolved URLs create one edge, while distinct section
fragments remain separate evidence. Self-links are permitted because reads are
bounded to one hop.

Only resolved HTTP(S) links without credentials are indexed. Existing structured
extraction supplies links and relative/base URL resolution, so no second crawler
frontier or HTML parser is introduced. Known boilerplate navigation stays excluded
by that extraction policy.

An edge can point to a page not yet indexed. SurrealDB's
[RELATE statement](https://surrealdb.com/docs/reference/query-language/statements/relate)
supports nonexisting endpoints unless `ENFORCED` is requested; Ariadne deliberately
keeps these edges to preserve explicit unresolved targets. Once the exact target
URL is indexed, its availability is visible without recrawling the origin.
`target_indexed` indicates retained document existence, including removed
records; `target_availability` distinguishes active, removed and unindexed.
Outgoing results include unresolved and removed targets so the original source
link remains inspectable. Incoming queries require an active indexed target;
removed origins never contribute results.

Redirect aliases and declared canonical hints are not guessed or merged during
traversal. Query canonical indexed URLs. A link to an alias remains unresolved
unless that exact URL is itself indexed. Cross-source reconciliation and
multi-hop traversal are future work.

## Entity policy

The policy `document-links-rust-paths-v1` extracts case-sensitive ASCII qualified
Rust-style identifiers of 2 or more `::`-separated components, up to 256 bytes.
Examples include `reqwest::Proxy`, `Proxy::custom` and `std::sync::Arc`.
Only inline code, code blocks and a section's own qualified heading contribute
mentions. Inline code inside lists, notes, quotes, links and table cells is
included. Ordinary prose and navigation do not contribute inferred entities.

Every mention is attached to a real chunk using its original section/block
range, with one edge per chunk and exact spelling. Section-heading mentions
apply to chunks containing that heading context. The shared `entity` record is
identified by kind and exact spelling; `mentions` is a chunk-to-entity relation
with a document reference for replacement and source filtering.

This is spelling evidence, not Rust symbol resolution: comments/string literals
inside code can contribute, and identical spellings in different sources need
not refer to the same definition. Full matches are indexed; `reqwest::Proxy::all`
does not automatically create a `reqwest::Proxy` mention. Unqualified names,
Unicode identifiers, SurrealQL commands, aliases, semantic equivalence and
LLM-assisted entity extraction are outside this first policy.

Each document derives at most 4096 links and 8192 mentions. Hitting a cap is
recorded in document graph metadata and source coverage. Existing raw and
structured content remains intact. Unreferenced entity identity records may
remain after replacement; they are not returned without current mention edges.

## Atomic updates and existing stores

Ingestion prepares graph evidence from the structured document and semantic
chunks before persistence. All graph queries and writes remain in `storage`.
Document replacement deletes obsolete outgoing links and mentions and writes
the new graph in the same transaction as chunks, sections, embeddings, audits
and crawl completion. Failed processing/writes preserve the previous graph.
Direct 404/410 removal deletes the origin's outgoing links and mentions;
incoming links from other pages remain explicit evidence of a removed target.
Incomplete discovery does not prune unvisited graph data.

304, raw-unchanged and normalized-unchanged recrawls retain compatible graph
records, including original content provenance. Normalized reuse preserves graph
metadata when raw/HTTP data is refreshed. A missing/older graph policy prevents
reuse, reprocessing retained HTML when the server responds 304. Stable chunk IDs
allow existing embeddings to be reused when their content/model still matches.
The source coverage counts make gradual upgrades observable.

Schema upgrades are additive. Existing documents are backfilled as recrawls visit
them. To index retained content offline, use the existing bounded command:

```sh
./target/debug/Ariadne reprocess reqwest graph-upgrade --max-pages 200
```

This processes the configured bounded seed set; inspect graph coverage afterward.
No automatic whole-database backfill or destructive migration runs on startup.

## References and validation

Spider's `Website::get_links` returns the crawl engine's visited set, rather than
per-document content-link provenance. Crawl4AI's `quick_extract_links` resolves
base URLs, deduplicates resolved links and distinguishes internal/external
links. Ariadne reuses its existing structured extractor for those inputs and
adds only knowledge persistence and bounded reads; no crawler changes or new
dependencies were needed.

Deterministic tests cover code/heading evidence and nested structures, derivation
caps, fragments and query preservation, boilerplate exclusion, unresolved targets
becoming indexed, source isolation, deterministic limits, replacement,
idempotence, persistence after restart, 304/hash reuse, removal/restoration,
legacy graph backfill and actual database rollback after graph writes. A real
MCP client tests both tools and invalid/unknown inputs.

```sh
cargo test --test graph --test recrawling --lib
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

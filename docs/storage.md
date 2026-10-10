# Embedded knowledge storage

Ariadne runs SurrealDB **in process**, backed by the Rust-native SurrealKV disk
engine. Users need no database executable, server, credentials, or migration CLI.
The application opens namespace `ariadne`, database `knowledge` automatically.
No network database listener is started.

## Versions and startup

The verified dependency target is SurrealDB `3.3.0` and SurrealKit
`1.0.0-beta.6`. SurrealKit is currently a prerelease and is pinned exactly.
Its published dependency requires SurrealDB 3.3.0; older examples in the online
documentation reference different SDK generations. Rust 1.95 or later is needed
by SurrealKit; this project is developed with Rust 1.98.1.

The default database lives under the platform application data directory:

- Linux: `$XDG_DATA_HOME/ariadne/knowledge`, normally
  `~/.local/share/ariadne/knowledge`.
- Override: `ARIADNE_DATA_DIR=/some/directory`; the database is created in its
  `knowledge` child directory.
- Library callers can use `KnowledgeStore::open(path)` for an explicit database
  directory, including isolated test directories.

Startup creates the directory, opens SurrealKV, selects the namespace/database,
and calls SurrealKit `Sync::embedded` against schema compiled with `include_str!`.
There are no runtime schema files to install. The explicit schema slice avoids
the macro's directory discovery/rebuild caveat; new files must be added to it.
Schema failure prevents the store being returned to the application.

Automatic sync uses `prune(false)`: deleting a definition from the repository
does not delete its database object. This is declarative schema management, not
an ordered data-migration history. Future destructive changes and data backfills
must use explicit SurrealKit rollouts, with upgrade/recovery tests; turning off
pruning does not make every field-definition change safe. Do not open an existing
database with an older app release as a downgrade procedure.

SurrealKV owns an exclusive datastore lock. Only one Ariadne process can use a
database directory at a time. The long-running MCP process owns that store; agents can inspect sources and run
crawl/recrawl jobs through its knowledge tools. Stop it before using the CLI
against the same directory. The SDK
begins asynchronous shutdown when its last handle is dropped. After a restart,
previous `running` crawls become `interrupted` and remain inspectable.

Database network targets are denied, and there is no raw-query API exposed to
callers. Crawled strings are bound values, never interpolated into SurrealQL.

## Knowledge boundary and records

`src/storage` owns all application database queries. `KnowledgeStore` exposes
registration, source listing, crawl begin/fail/finish/inspection, document loading,
and page outcome inspection. No generic repository hierarchy is added yet.

Records use deterministic identities and actual SurrealDB record references:

| Table | Identity | Purpose |
| --- | --- | --- |
| `source` | caller's source ID | Name, root URL, creation time |
| `crawl_run` | `[source_id, crawl_id]` | Request configuration, status, timestamps, counters, blocked URLs, errors |
| `page_outcome` | `[source_id, crawl_id, sequence]` | Raw page/provenance and extraction success/rejection |
| `document` | `[source_id, fetched_canonical_url]` | Current successful document metadata and raw page |
| `section` | `[source_id, document_url, local_section_id]` | Ordered blocks, headings, anchors, parent section reference |
| `chunk` | stable source/URL/section/content/policy hash | Retrieval text, Markdown, block range, provenance, document/section references |

Tables are schemafull. Flexible object fields preserve the complete evolving
domain representation, including nested content blocks and byte arrays. Section
records are authoritative for hierarchy; document metadata omits the section
array. Reads reassemble both in one transaction. This initial design intentionally
retains raw content in both successful page audit and current document records;
deduplicated raw blobs can follow if storage measurements justify them.

Registering an identical source ID/name/root returns the original record.
Conflicting metadata fails. Crawl IDs cannot be reused within a source.
`begin_crawl` persists the validated request before fetching starts.
`finish_crawl` commits page audits, successful document replacement, section
and chunk replacement, and run completion in one transaction. A rejection records the raw
failed page without overwriting the previous good document. A failed transaction
leaves the run running so its caller can record the error with `fail_crawl`.
Identical normalized redirect aliases share one document and retain separate page audits;
the direct requested URL is preferred for current document provenance. Conflicting
representations of one canonical URL in a batch fail rather than selecting an
arbitrary winner. Distinct URLs and sources retain separate provenance even when
their normalized hashes match. See `chunking.md` for content identity and policies.

Completed means the execution finished, not that every URL in a source was
discovered. Delivery loss, audit overflow, robots exclusions, and budget limits
must still inform later recrawl logic. Missing documents are never deleted by
this initial storage implementation.

Whole-batch transactions match the current bounded, in-memory crawl adapter.
Large sources will require measured batch sizes and checkpointed ingestion;
there is no streaming storage implementation yet. Semantic chunking and hashes
are persisted now, and BM25 full-text indexes support lexical retrieval. Embedding
storage and vector search are subsequent roadmap work. See `retrieval.md`.

## Running the vertical slice

```sh
cargo run -- --help
cargo run -- source add reqwest 'reqwest documentation' https://docs.rs/reqwest/latest/reqwest/
cargo run -- source list
cargo run -- crawl reqwest initial
cargo run -- run reqwest initial
cargo run -- document reqwest https://docs.rs/reqwest/latest/reqwest/
cargo run -- chunks reqwest https://docs.rs/reqwest/latest/reqwest/
cargo run -- indexing reqwest https://docs.rs/reqwest/latest/reqwest/
cargo run -- search 'Proxy::custom' --source reqwest --limit 5
```

The crawl command uses the existing conservative defaults: same origin, source
path prefix, 100-page budget, concurrency two, robots enabled. Use a fresh crawl
ID for each attempt. Browser rendering and configurable CLI crawl limits remain
future work. Commands emit structured JSON; library users receive typed results.

For backups at this stage, stop Ariadne and copy the entire database directory.
There is no live-backup or restore command yet.

Embeddings use model-scoped tables and vector indexes managed through bundled
SurrealKit schema templates. They preserve reuse and invalidate obsolete vectors
with document replacement; see `embeddings.md` for state and migration policy.

References: [SurrealDB embedding](https://surrealdb.com/docs/reference/rust/embedding),
[SurrealKit embedded/library support](https://github.com/surrealdb/surrealkit/tree/main/crates/surrealkit),
[schema sync and rollouts](https://surrealdb.com/docs/manage/schema-migration).

## Backup and restore

Use a **closed-store directory backup** for this embedded SurrealKV deployment.
Stop the MCP/application process cleanly and wait for it to exit before copying.
Do not copy live datastore files: this application does not implement online
snapshot coordination. Record the application commit/version and embedding model
revision with the backup. Copy the whole `knowledge` directory, including all
nested files, rather than selecting particular engine files.

For example, with an explicit data directory and the application stopped:

```sh
cp -a /your/ariadne-data/knowledge /your/backup/knowledge
```

Restore into a fresh directory, preserving the original backup:

```sh
mkdir -p /your/ariadne-restored
cp -a /your/backup/knowledge /your/ariadne-restored/knowledge
ARIADNE_DATA_DIR=/your/ariadne-restored ./target/debug/Ariadne source list
ARIADNE_DATA_DIR=/your/ariadne-restored ./target/debug/Ariadne source status SOURCE_ID
```

Open the copy with the same application version first, inspect sources and crawl
status, and run a known lexical search before switching MCP to the restored data
directory. Keep the old directory until verification succeeds. A model must still
be installed locally to perform vector/hybrid queries; vectors and model identity
are in the database, model weights are not. Startup applies bundled SurrealKit
schema sync and marks unfinished jobs interrupted. Test an upgrade on a restored
copy before opening an important original database with a new release.

The step-12 schema adds `crawl_job`, the cancelled run status, and a source
admission revision that serializes concurrent job reservations without changing
source provenance. Upgrade tests
verify that an earlier schema retains sources, abandoned jobs become interrupted,
and copying a closed datastore preserves job/source records. The earlier full-text
and chunk schema upgrade tests also remain in the suite. This is targeted upgrade
coverage, not a guarantee for arbitrary future schema changes or engine downgrades.

Document revision history adds immutable `document_revision`, `revision_section`
and `revision_chunk` tables and a current representation pointer. Backfill occurs
on the next successful per-document ingestion/recrawl/reprocess, inside the same
transaction as current data replacement. Startup does not scan/rewrite documents.
See [document revisions](document-revisions.md) for identity, retrieval and
retention limits. These tables are included in whole-directory closed-store
backups.

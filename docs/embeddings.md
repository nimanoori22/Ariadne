# Embeddings and vector retrieval

Ariadne stores embeddings as derived data alongside persistent semantic chunks.
The first real provider is local Ollama, with `embeddinggemma:latest` as the
default model. The database remains embedded; Ollama is a separate inference
service. Ariadne does not install models or start Ollama as a side effect of a
crawl. Lexical search works without it.

## Usage

Install Ollama and start its local service, then pull the model explicitly:

```sh
ollama pull embeddinggemma
cargo run -- embed reqwest
cargo run -- embeddings spaces
cargo run -- embeddings status reqwest
cargo run -- vector-search 'How can I route HTTP traffic through an intermediary?' --source reqwest --limit 5
```

`ARIADNE_OLLAMA_URL` defaults to `http://127.0.0.1:11434/`.
`ARIADNE_EMBED_MODEL` defaults to `embeddinggemma:latest`. Set these variables
for a different local endpoint/model. The provider currently embeds raw chunk
Markdown and plain query text; models requiring task-specific prefixes need a
provider formatting policy before use. URL credentials, query strings,
fragments and redirects are rejected.

`embed` returns a JSON report with scanned, reused, generated, failed and
superseded counts, plus vector-space ID. A partial failure prints the report
and exits unsuccessfully. Inspect status, correct the problem and rerun the same
command; ready vectors are reused, pending/failed work is retried.

The status command reports current ready/pending/failed/missing chunk counts
and historical stale records for each known model space. A source that has not
been embedded has no registered space yet. The library's `embedding_coverage`
can also report missing chunks for an unregistered space.

## Boundaries and identity

`EmbeddingProvider` supplies a space descriptor, resource limits and batched
embedding operation with document/query purpose. A deterministic provider in
tests verifies orchestration without weights or external calls. Normalization,
response validation and checkpointing are independent of Ollama.

`EmbeddingSpace` records provider, model, immutable revision, dimensions and
input policy version. Its SHA-256 identity selects a separate table/index, so
different models, weight revisions, dimensions or formatting versions never
share a vector search. The Ollama endpoint is not part of vector identity:
the same installed weights and input policy are reusable across endpoints.

Ollama's installed manifest digest identifies the revision. When its tags API
lists multiple runner variants under one name, the provider uses the uniquely
selected manifest from `/api/show`, not listing order. A first-load conversion
is permitted only during the unpersisted dimension probe; its output is
discarded and reprobed under the new stable identity. Subsequent requests check
identity before and after inference. Mid-session changes fail explicitly.

Dimension count is discovered with an unpersisted probe and must be 1..4096.
Every response must contain exactly one finite, nonzero vector of that dimension
per input. Vectors are L2-normalized before storage/querying.

## Batching, limits and failures

Generation uses sequential batches: at most one provider request at a time.
The initial Ollama limits are 16 inputs, 8192 bytes per input and 65536 input
bytes per batch. HTTP responses are capped at 4 MB (model metadata at 1 MB).
Requests have a 5-second connection timeout and a 30-second total HTTP timeout;
the provider boundary imposes a 120-second embedding-call deadline.

429 and 5xx responses receive at most three attempts with short exponential
delays. Numeric `Retry-After` values up to five seconds are honored; longer or
unsupported delays fail for later retry rather than retrying early. Connection
and metadata errors fail the operation. A failed multi-input batch is retried
one input at a time to isolate model rejection or oversized inputs. Single-input
failures are not repeated by orchestration. Server error bodies and malformed
payload values are not exposed in stored errors.

Byte budgets are resource caps, **not token counts**. Ollama receives
`truncate: false`; a model-context rejection is recorded as a failed embedding.
Oversized atomic code/table chunks remain intact and lexically searchable.
Per-chunk `token_count` remains unknown: the API exposes aggregate token usage,
not a matching local tokenizer or per-input counts. No silent truncation or
character-to-token estimate is used.

## Persistence and recrawling

The core schema registers model spaces. A compiled-in vector schema template
is applied through SurrealKit, in a module scoped to the complete space ID.
Only generated hash identifiers and validated dimension integers become schema
substitutions. Startup syncs registered spaces without deleting older ones.

Each derivative references its chunk/document and stores content hash, status,
attempt count, creation/update times and bounded failure details. Preparation
checkpoints pending work; successful completion installs the vector and ready
state together. Cancelled/stopped generation leaves pending work resumable.
Missing means a current chunk has no compatible derivative yet.

Document replacement marks obsolete derivatives stale and removes their
vectors in the same transaction that replaces sections/chunks. Identical
stable chunks retain ready vectors and search uses current crawl provenance.
Late inference checks chunk identity/hash before committing. A failed producer
cannot overwrite a concurrent producer's ready vector. Failed extraction keeps
the last good documents and derivatives through the existing ingestion policy.

Changing the model/revision/dimension creates a new space; re-embed each source
and query it explicitly through that provider. Earlier spaces remain usable.
Automatic old-space deletion/retention policy is deferred. Concurrent recrawling
can move chunks behind a generation pass's cursor: rerun `embed` to fill missing
work. Durable background scheduling is part of step 9/later operations work.

## Retrieval and validation

`retrieval::vector_search(&store, &provider, query)` embeds a query and returns
the same bounded source-data hits as lexical retrieval, with `match_kind: vector`
and the complete embedding-space descriptor. Scores are cosine similarity in
[-1, 1], not BM25 or probabilities. Search returns only ready, current derivatives;
partial indexing does not imply full source coverage. Check coverage separately.

Unfiltered search uses model-scoped HNSW (`COSINE`, F64, EFC 150, M 12,
query effort 100). It is approximate. Source-filtered search uses the source
index and exact cosine scoring within that source. Metadata filters also use
exact scoring before the result limit; metadata-only searches can scan more vectors. On pinned SurrealDB 3.3.0,
our crowded duplicate-vector fixture returned only two of three requested
source hits with filtered HNSW; the exact path fills all three. This observation
does not characterize every filtered HNSW query or future engine version.
Filtered scoring costs grow with the selected source's ready vector count.

Results keep title, heading path, anchors, crawl timestamp, source/chunk IDs and
hashes. Text is a bounded Unicode prefix, with explicit truncation. Ties are
ordered by source, URL, sequence and chunk ID; approximate candidate membership
can vary. MCP search and [hybrid ranking](hybrid-retrieval.md) are implemented.
[Incremental recrawling](recrawling.md) avoids regenerating unchanged derivatives.

The deterministic suite covers model/dimension isolation, source filtering,
restart/reuse, failure recovery, invalid vectors, partial input rejection,
cancellation, late inference, CLI operations and Ollama protocol compatibility.
Run the opt-in real-model check with your explicitly configured local service:

```sh
cargo test --test embeddings real_ollama_semantic_smoke -- --ignored --nocapture
```

The implementation was also checked with Ollama 0.40.0 and embeddinggemma
revision `82f094e4c0e19bc4208c692996d4f1ecf14e82a8dee579ee1d2ca1175260058e`,
768 dimensions. The query “How can I send HTTP traffic through an intermediary
server?” returned the fixture's Proxy section; all-term lexical search returned
no hit. This is a semantic smoke test, not a broad relevance benchmark.

References: [Ollama embedding API](https://docs.ollama.com/api/embed),
[installed model identity](https://docs.ollama.com/api/tags),
[Ollama 0.40 manifest selection types](https://github.com/ollama/ollama/blob/v0.40.0/api/types.go),
[SurrealDB vector indexes](https://surrealdb.com/docs/learn/data-models/vector-search/vector-indexes).

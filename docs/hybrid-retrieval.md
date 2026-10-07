# Hybrid retrieval and context expansion

Step 11 adds hybrid search, metadata filters, and bounded context assembly in
the retrieval layer, with shared CLI and MCP access. Storage owns all SurrealQL;
Spider remains the crawl engine. No new dependencies or database service are
needed.

## Usage

With a registered, indexed source and local Ollama configured:

```sh
./target/debug/Ariadne hybrid-search 'Proxy::custom' --source reqwest --limit 5
./target/debug/Ariadne retrieve 'How do I route requests through an intermediary?' --source reqwest --context-chars 12000
```

`hybrid-search` returns ranked chunk hits. `retrieve` returns context passages
with matched-candidate evidence and nearby chunks. It defaults to hybrid search;
use `--retrieval-mode lexical` for retrieval without Ollama, or `vector` for
semantic candidates alone.

```sh
./target/debug/Ariadne retrieve 'Proxy::custom' --retrieval-mode lexical --source reqwest --neighbors 1 --context-chars 8000
./target/debug/Ariadne search 'configuration' --url-prefix https://example.test/docs/client --heading 'Proxy configuration' --crawled-after 1791360000
```

`--source`, `--url-prefix`, `--heading`, and `--crawled-after` apply to lexical,
vector and hybrid candidates **before limits/ranking**. URL scope includes the
specified page and its path descendants, respects path component boundaries,
and accepts only HTTP(S) URLs without credentials, query or fragment. Heading
matching checks any ancestor/current heading with case-insensitive equality.
The timestamp is an inclusive minimum content-fetch time in Unix seconds;
it does not mean the last successful HTTP revalidation time.

`retrieve` also accepts `--context-chunks` (default 50, maximum 100),
`--context-chunk-chars` (default 4000, maximum 20000), `--context-chars`
(default 12000, maximum 100000) and `--neighbors` (default 1, maximum 3).
These budgets apply to context source text; provenance metadata is separate.

## Fusion and candidate limits

The default ranker delegates to SurrealDB 3.3.0's built-in
[`search::rrf`](https://surrealdb.com/docs/reference/query-language/functions/database-functions/search#searchrrf).
It uses ranks rather than adding BM25 and cosine scores with incompatible
scales. Each list contributes `1 / (60 + rank)`, where the first rank is 1.
Original component ranks and scores remain in each hit's `fusion` evidence;
`score` is the fused value and `match_kind` is `hybrid`. It is not a probability.
The generic `HybridRanker` boundary allows an alternative strategy without
changing crawling, embeddings or the MCP transport.

Both candidate generators use the same filters. Candidate depth is four times
the requested limit, with a minimum of 20 and a maximum of 50 per list.
Lexical candidates retain the existing identifier-aware exact matching policy;
semantic candidates query one compatible embedding space. Chunks appear once
in the fused output. The full bounded union is fused before deterministic tie
resolution and final truncation, avoiding arbitrary top-k membership at ties.

Tie resolution is deterministic for the returned candidate pool; HNSW candidate
membership remains approximate. Unfiltered vector candidates retain HNSW. Queries with source or metadata
filters use exact cosine scoring within the matching records, addressing the
sparse-filter recall limitation demonstrated on the pinned engine. Source
filters retain their indexed partition; metadata-only exact scoring can scan
more ready vectors and keeps the existing ten-second query timeout.

Hybrid search requires an indexed embedding space and a working provider.
Provider failures are surfaced rather than silently changing the mode or its
score interpretation. Lexical search and lexical context retrieval remain
available independently. Partial embedding coverage still yields partial
semantic recall; inspect `embeddings status` separately.

## Context and provenance

Context is assembled from a single read transaction across all selected hits.
A hit is expanded only if its current chunk still has the same source, document
and content hash. `skipped_stale_hits` reports hits replaced between search and
expansion, rather than substituting newer content silently.

The assembler includes neighboring chunks within the same document, including
adjacent sections, while retaining each chunk's heading path, section anchor,
sequence, block range, source URL, crawl timestamp and hash. Existing semantic
chunking keeps code and its explanation together; context therefore preserves
that group when it fits the budget. Parent headings are provided in
`heading_path`; it does not load entire parent sections or unrelated documents.
Metadata filters select matches, while their nearby context may have another
heading in the same document.

Matched chunks receive budget first, followed by the nearest neighbors.
Overlapping neighborhoods are deduplicated by chunk ID. Passages group chunks
by source/document and order them by document sequence; gaps remain visible in
those sequences. Source text stays labeled `source_data`, never agent
instructions. Truncated Unicode prefixes are marked `text_truncated`, and
`budget_exhausted`, `omitted_chunks`, `text_omitted` and `deduplicated_chunks`
make incomplete context explicit. Stored content is never truncated by this
response policy; hashes describe the complete stored chunks.

MCP keeps the existing `search` tool and lexical default. Example arguments:

```json
{
  "query": "Proxy::custom",
  "mode": "hybrid",
  "source_id": "reqwest",
  "limit": 5,
  "filter": {
    "url_prefix": "https://example.test/docs/",
    "heading": "Proxy configuration"
  },
  "context": {
    "neighbor_chunks": 1,
    "max_total_chars": 8000
  }
}
```

The response retains `mode` and `hits`, adding `context` only when requested.
The hit-text budget and context-text budget are independent. Existing request
limits, provider concurrency, cancellation, timeouts, stderr tracing and
sanitized provider-error responses remain in force.

## Evaluation and verification

The checked-in deterministic relevance fixture covers exact API identifiers,
semantic paraphrases, a query whose useful answers are split between lexical
and vector retrieval, and SurrealQL syntax. Mean recall at two results is:

| Mode | Fixture recall@2 |
| --- | ---: |
| Lexical | 0.625 |
| Vector | 0.625 |
| Hybrid | 1.000 |

This test uses deliberately complementary deterministic vectors. It verifies
fusion and measures improvement on this fixture; it is not a real-model or
broad retrieval-quality benchmark. Real-model evaluation on a larger query set
remains useful before tuning candidate depth, rank constants or new strategies.
The fixture's relevant-page labels live in `tests/hybrid_retrieval.rs`.

```sh
cargo test --test hybrid_retrieval -- --nocapture
cargo test --test knowledge_mcp cli_ingestion_restart_and_real_mcp_client_search_both_modes
```

Tests cover built-in fusion, stable ties, component evidence, replaceable
ranking, filtered recall, invalid filters, overlapping context, code and heading
preservation, global Unicode budgets, stale-hit detection, CLI retrieval and
MCP hybrid/context behavior after restart.

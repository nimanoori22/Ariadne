# Graph-assisted retrieval

Graph assistance is an opt-in addition to lexical, vector and hybrid retrieval.
It uses the existing document-link/entity graph to add source-backed candidates,
then fuses ranks through the existing SurrealDB ranking boundary. It needs no new
dependency or database schema.

## Usage

```sh
./target/debug/Ariadne graph-search 'Proxy::custom' --retrieval-mode lexical --source reqwest --limit 5
./target/debug/Ariadne graph-search 'How do I configure outbound connections?' --source reqwest --limit 5
./target/debug/Ariadne retrieve 'Proxy::custom' --retrieval-mode lexical --source reqwest --graph --context-chars 8000
```

`graph-search` defaults to hybrid retrieval, returning `hits` and `graph`
diagnostics. Use `--retrieval-mode lexical` for operation without Ollama. Vector
and hybrid modes retain their indexed-model/provider requirements. `retrieve
--graph` assembles bounded context and adds the same diagnostics; graph path
and fusion evidence are retained in each passage's `matches`.

All existing source, metadata, result and text-budget options apply. Graph CLI
options are:

| Option | Default | Range |
| --- | ---: | --- |
| `--graph-seeds` | 4 | 1–8 |
| `--graph-edges` | 4 | 1–8, separately for link edges and seed entity identities |
| `--graph-chunks` | 2 | 1–3 per edge/entity identity |
| `--graph-candidates` | 20 | 1–50 |
| `--graph-links` | true | true / false |
| `--graph-entities` | true | true / false |

On `retrieve`, supplying a graph option also enables graph assistance. At least
one of links/entities must be enabled. Options are validated before opening an
embedding provider. Ordinary `search`, `hybrid-search` and `retrieve` retain
their previous behavior when graph assistance is not requested.

MCP keeps the existing `search` tool and its lexical default. Supply a `graph`
object to enable assistance; omitted fields take the defaults above:

```json
{
  "query": "Proxy::custom",
  "mode": "lexical",
  "source_id": "reqwest",
  "limit": 5,
  "graph": {
    "max_seeds": 4,
    "max_edges_per_seed": 4,
    "max_chunks_per_edge": 2,
    "max_candidates": 20,
    "links": true,
    "entities": true
  },
  "context": {"neighbor_chunks": 1, "max_total_chars": 8000}
}
```

An empty `graph: {}` enables the defaults. Unknown keys and out-of-range limits
are rejected before provider work. The response keeps `mode`/`hits` and adds
`graph` diagnostics; `context` remains optional. The MCP tool count stays eleven.

## Candidate generation and ranking

Base retrieval uses the same bounded candidate depth as hybrid retrieval: four
times the requested result limit, clamped to 20–50. The base candidate lists are
fused to choose up to `max_seeds` seed chunks. Graph reads then execute in one
read transaction, with a ten-second timeout and independent fanout limits.

Only one hop is expanded:

- **Outgoing links:** from the seed's document to another active indexed document
  in the same source. A fragment targets that section's chunks; absent fragments
  select the document's earliest chunks. Encoded fragments are compared against
  the chunk's encoded source URL. Unknown fragments return no candidate.
- **Incoming links:** active same-source pages linking to the seed's document.
  Candidates come from the referring page's earliest chunks; the graph currently
  records document-level link provenance, not the exact block containing the link.
- **Shared entities:** chunks in another same-source document mentioning an exact
  qualified entity also mentioned by the seed chunk. Both mentions must use the
  current policy, and mention hashes must match the current chunks.

Self-document links/mentions do not add graph candidates. Missing/removed link
targets and removed origins are excluded. No network request follows a relation,
no alias reconciliation occurs, and candidates are never recursively expanded.
Even an unfiltered multi-source query cannot traverse between sources: each
seed's expansion stays within that seed's source. Identical entity spellings in
other sources are not considered neighbors.

Metadata filters apply to base seeds and to landing chunks **before chunk
limits**, including the inner shared-entity candidate lookup. Link/entity
identity fanout is bounded independently, so a qualifying link beyond an edge
budget can still be omitted; diagnostics report this partial traversal.

Rows are interleaved across edge groups before later chunks in those groups,
deduplicated by chunk ID, then capped by `max_candidates`. Each candidate keeps
up to four distinct paths; duplicate paths do not cast extra rank votes.
Document ordering/sequence and existing stable tie rules make results
reproducible on the same candidate snapshot.

The lexical, vector and graph lists are combined with the existing
[`search::rrf`](https://surrealdb.com/docs/reference/query-language/functions/database-functions/search#searchrrf)
ranker (constant 60). Graph ranks are derived from bounded seed/edge order, not
an embedding, PageRank, learned relevance score or a count of incoming links.
Graph candidates do not need an embedding, though only candidates present in
another list receive that list's vote. `fusion.graph_rank` explains their rank
in the added list; lexical/vector component evidence remains intact. With graph
candidates, final `match_kind` is `graph` and `score` is RRF. Scores are not
probabilities. Hits without graph paths can still be returned from the base
lists. If no eligible graph candidates exist, the original mode's scoring and
match-kind semantics are preserved.

This remains an optional retrieval strategy: a link or shared spelling is useful
evidence, but it can also connect an irrelevant page. No general relevance
improvement is assumed, and graph assistance is not enabled by default.

## Provenance, coverage and budgets

Every hit retains the normal source/document/section/chunk URL, content hash,
content crawl timestamp and bounded source text. Each graph-supported hit adds
`graph.paths`, containing the relation kind, seed chunk ID/hash/URL, and either
the link's originating document/target URL or the exact shared entity spelling.
This describes why the candidate was visited; it is not a claim that the seed
chunk itself contains a document-level link. Source text and entity labels
remain untrusted data. `paths_truncated` records more than four distinct paths.

Response diagnostics include:

- `index_version`, `seeds`, `seeds_truncated`: the graph policy and selected seed
  count, with whether the bounded base pool had more potential seeds.
- `skipped_stale_seeds`: seed chunks missing, replaced or removed before the graph
  snapshot. Those proven stale seeds are also removed from the fused base lists.
- `unindexed_seeds`, `truncated_seed_indexes`: missing/older graph policy or capped
  ingestion derivation on selected seed documents.
- `traversal_truncated`: link/entity identity or per-edge chunk budget was exceeded.
- `candidates_truncated`, `candidates`: the bounded graph list size and whether
  more deduplicated candidates existed in the traversed snapshot.

These are selected-seed diagnostics, not a complete source coverage audit.
`get_links`/`find_entity` provide source-level graph coverage. Empty graph results
cannot imply no relationships exist when coverage or budgets are incomplete.
Old documents can be backfilled through recrawl or offline `reprocess`; see
[knowledge graph](knowledge-graph.md).

Concurrent replacement cannot fuse inconsistent hashes for the same candidate
ID. Context assembly independently checks current chunks, reports stale hits,
and uses its existing global text/chunk budgets. There is no database snapshot
spanning provider inference, base retrieval, graph expansion and context assembly;
path hashes identify the seed evidence observed during graph expansion.

## Evaluation and validation

The original four-query relevance fixture remains unchanged in its content and
labels. A separate three-case fixture covers outgoing section links, incoming
links and exact shared entities. It uses synthetic marker queries and deliberate
deterministic embeddings that retrieve a correct seed while placing its related
answer behind irrelevant pages.

| Query set | Lexical recall@2 | Vector recall@2 | Hybrid recall@2 | Graph-assisted hybrid recall@2 |
| --- | ---: | ---: | ---: | ---: |
| Original four queries | 0.625 | 0.625 | 1.000 | 1.000 |
| Three relation cases | 0.500 | 0.500 | 0.500 | 1.000 |

The original set has no usable graph neighbors, verifying unchanged fallback
ranking. The relation set demonstrates that graph evidence can recover relevant
linked/entity-sharing chunks. These small synthetic measurements validate the
mechanism, not broad semantic quality, precision, a real model or usefulness on
arbitrary documentation. A larger relevance set with noisy graph relationships
and real embeddings is needed before choosing defaults or tuning weights.

Tests also cover encoded anchors, same-source entity isolation, filters before
entity fanout limits, provider-independent lexical access, disabled relation
kinds, high-fanout and candidate/path truncation, text/context budgets, stale,
removed and incompatible graph records, duplicate candidates, repeatability,
and actual CLI restart/MCP clients.

```sh
cargo test --test graph_retrieval --test hybrid_retrieval -- --nocapture
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

Crawl4AI's inspected BM25 content filter applies query/token relevance and tag
weights during extraction; Spider's visited-link API describes crawl discovery.
Neither boundary provides the persisted retrieval graph required here. Ariadne
keeps those crawl/extraction concerns in place, reuses its graph records and the
pinned SurrealDB rank-fusion/URL-fragment capabilities, and confines new SurrealQL
to `storage`.

# Lexical knowledge retrieval

Ariadne now supports persistent full-text search over indexed documentation,
without an embedding provider or a separate search service.

Vector retrieval is available separately; see `embeddings.md`. Lexical search
continues to work when Ollama is unavailable.

```sh
cargo run -- search 'SOCKS5 proxy support'
cargo run -- search 'Proxy::custom' --source reqwest --limit 5
cargo run -- search 'DEFINE INDEX' --source surreal --mode exact
```

The command returns JSON knowledge hits with source ID/name, title, section path,
source URL and anchor, crawl ID/timestamp, chunk ID/hash, source block range,
matched text, relevance score, match kind, and truncation status. Every hit is
labelled `content_kind: source_data`; crawled instructions remain untrusted data.

## API and boundaries

`retrieval::search(&store, SearchQuery::new(query))` is the public knowledge API.
The retrieval module validates requests and handles matching/ranking policy.
Storage owns the SurrealQL and keeps the embedded SDK private. No new repository
hierarchy, external database, or raw-query endpoint is introduced.

`SearchQuery` supports:

| Option | Default | Allowed |
| --- | --- | --- |
| `query` | required | Nonempty, contains an alphanumeric character, at most 1,024 bytes |
| `source_id` | all sources | Optional exact source ID; unknown sources yield no hits |
| `limit` | 8 | 1–50 |
| `max_text_chars` | 4,000 | 1–20,000 Unicode characters per hit |
| `mode` | `auto` | `auto`, `keywords`, `exact` |

Queries without words, invalid limits/budgets, empty source filters, and nonwhite-
space control characters are rejected. Query values, source filters, and generated
exact patterns are bound parameters. A search string cannot execute SurrealQL or
provide an arbitrary regex.

The CLI also accepts `--source`, `--limit`, `--mode`, and `--max-chars` with values.
Queries containing spaces must be quoted by the shell.

## Analyzer and match semantics

Bundled SurrealKit schema defines a versioned `documentation_lexical_v1` analyzer
with `TOKENIZERS blank, punct FILTERS lowercase`, and BM25 full-text indexes on
chunk text and document title. Heading paths are already part of chunk text.
No stemming, stopword removal, ASCII folding, CamelCase splitting, or n-grams are
enabled. This preserves identifiers like `ClientBuilder` and words like `SOCKS5`.
It is word-based lexical matching, not natural-language understanding or semantic
search. Inflections and synonyms are not expanded automatically.

The pinned SurrealDB 3.3.0 experiment shows punctuation is itself tokenized:

```text
Proxy::custom() ClientBuilder SOCKS5
→ proxy, :, :, custom, (, ), clientbuilder, socks5
```

Full-text matching requires query tokens but does not enforce identifier adjacency.
For example, `Proxy::custom` can match a chunk with separate words `Proxy` and
`custom`, plus another namespace reference such as `Trait::method`.

- `keywords`: all analyzed query terms must match within one indexed field.
- `exact`: indexed candidates must also contain the case-insensitive literal
  phrase at identifier boundaries. Regex metacharacters in input are escaped.
  Whitespace/order inside the literal phrase matter. `Proxy::custom` can match
  `reqwest::Proxy::custom()`, but not `Proxy::customized()`.
- `auto`: a standalone qualified identifier (`Proxy::custom`, including an optional
  trailing `()`) gets the exact check; other input uses keyword matching.

When an identifier is embedded in a longer question, auto does not extract it as
an independent required phrase. Use a concise query or select exact mode when
literal phrase matching is intended.

Text and title are searched independently, each with source and exact filters
applied **before** ranking/limit. An experiment with a single OR query over the
two indexes on this pinned engine returned partial-term matches; the separate
queries preserve the tested all-term contract. Both run inside one transaction
to share a document/index snapshot during concurrent replacement.

The retrieval layer merges candidates by chunk ID, keeps the strongest field's
BM25 score, then orders by score descending, source ID, document URL, sequence,
and chunk ID. Each field returns at most the requested limit, and the merged
response is limited again. Titles and body can contribute hits independently;
query terms distributed across different fields do not satisfy an all-term query.
There is no vector/graph fusion, reranking model, or cross-chunk context expansion.

BM25 is a relevance score, not a probability. SurrealDB clamps common terms' inverse
document frequency at zero, so small/common-term corpora can produce valid matches
with a zero score. These hits are retained and deterministically ordered. Scores
use corpus-wide index statistics even when results are filtered by source.

## Persistence and bounded content

On startup, SurrealKit installs the analyzers/indexes. The embedded engine indexes
existing chunk records during the schema upgrade; those chunks are searchable
without recrawling. Older documents that never acquired chunks remain readable
but need successful processing before they participate in search.

Document/chunk replacement updates both full-text indexes in the same existing
storage transaction. Failed extraction retains the last good searchable data.
A database error rolls back document/chunk/index replacement together.

The query projects only result/provenance fields; raw HTML and the full Markdown
payload are not returned. Text is a Unicode-safe **prefix** of the chunk, limited
inside the database with `string::slice`, and `text_truncated` marks omitted text.
This is not a match-centered snippet: an oversized chunk's match can occur beyond
the returned prefix. Use the existing document/chunk inspection APIs for complete
content. Provenance fields are retained, not truncated into invalid URLs or IDs.
Each indexed SELECT has a fixed 10-second query timeout.

Verification covers exact identifiers and distractors, lexical queries, title-only
matches, source filtering, ordering/limits, common-term zero scores, Unicode text
budgets, source text containing instructions, query binding, recrawl/index updates,
failed replacement, schema upgrade with existing chunks, CLI restart, and the
local crawl → extraction → chunks → restart → search path.

Embeddings and vector retrieval are implemented separately; see `embeddings.md`.
The next roadmap step connects ingestion orchestration and MCP search.

Primary references: [analyzers](https://surrealdb.com/docs/reference/query-language/statements/define/analyzer),
[search indexes](https://surrealdb.com/docs/learn/data-models/full-text-search/search-indexes),
[scoring and common-term zero scores](https://surrealdb.com/docs/learn/data-models/full-text-search/scoring-and-ranking),
[search functions](https://surrealdb.com/docs/reference/query-language/functions/database-functions/search).

# Agent operations and reliable local jobs

Step 12 extends the stdio knowledge server with document reads, source inspection,
and supervised crawling. Agents retrieve source-backed data through knowledge
operations; the database stays private.

## Setup

Register sources using `source add` before starting the MCP process. The MCP
process owns the embedded datastore exclusively. All read tools are available by
default. Administrators enable crawling for specific registered source IDs:

```json
{
  "command": "/absolute/path/to/Ariadne/target/debug/Ariadne",
  "args": ["mcp"],
  "env": {
    "ARIADNE_DATA_DIR": "/your/durable/data",
    "ARIADNE_MCP_CRAWL_SOURCES": "reqwest,surrealdb",
    "ARIADNE_OLLAMA_URL": "http://127.0.0.1:11434/",
    "ARIADNE_EMBED_MODEL": "embeddinggemma:latest"
  }
}
```

Only the administrator can register or change sources. Crawl tools take source
IDs, never arbitrary seed URLs, credentials, SQL, or network policy overrides.
The allowlist grants crawl permission, not a source-specific read permission.
This is a local stdio service: the process owner's permissions are the trust
boundary. There is no HTTP listener or remote authentication implementation.
A future remote transport must add authentication and per-client authorization
before deployment; do not expose this stdio process through an unauthenticated
network bridge.

MCP crawls default to public network destinations. Literal addresses and alternate
numeric IP spellings are checked before admission. The DNS resolver rejects
private/reserved addresses at connection time, including mixed public/private
answers. Proxies are disabled; same-origin component-aware path scope applies to
page links and redirect hops. Local Ollama is an administrator-configured separate
embedding service and is not governed by the crawl address policy.

For an explicitly trusted internal documentation source, the administrator can set
`ARIADNE_MCP_ALLOW_PRIVATE_NETWORK=1`. This relaxes the address policy for all
allowlisted sources in that process; it cannot be requested by a tool caller.
Keep that allowlist narrow. Trusted foreground CLI crawls retain their existing
network behavior.

## Tools

All tools reject unknown arguments. Runtime failures give compact errors; detailed
implementation errors are logged on stderr. Returned page text and headings are
untrusted source data, never agent instructions.

| Tool | Arguments and result |
| --- | --- |
| `search` | Existing lexical/vector/hybrid search, metadata filters and bounded context |
| `list_sources` | Optional `limit` (default 20, maximum 100) and `after` source-ID cursor; returns `sources`, `next_cursor` |
| `source_status` | `source_id`; document availability counters, latest crawl/stage, ten recent jobs |
| `get_document` | `source_id`, canonical `url`, optional `max_text_chars` (default 4000, maximum 20000); indexed chunks and headings |
| `get_section` | Same arguments plus numeric `section_id` from a document/search hit; one section |
| `crawl` | `source_id`, optional `max_pages` (default 100, maximum 200), `lexical_only` (default false), `discover` (default false); returns `job` |
| `recrawl` | Same arguments; conditional fetches and deterministic reuse of unchanged derived data |
| `job_status` | `job_id`; durable `job` and optional `run`, including ingestion stage and counters |
| `cancel_job` | `job_id`; requests cancellation, waits up to five seconds for cleanup, returns latest status |

Document/section reads perform no network request. They use one database snapshot,
cap text across the whole response, and return at most 100 chunks and 100 heading
records with explicit `truncated` metadata. Raw HTML, body bytes and HTTP headers
are omitted. Parent IDs, anchors, chunk IDs, content hashes and crawl timestamps
preserve provenance. A removed document remains inspectable with its availability
metadata; ordinary search already excludes removed documents. Text budgets cover
text, not all response metadata bytes.

Example agent workflow:

```json
{"name":"crawl","arguments":{"source_id":"reqwest","max_pages":100,"discover":true}}
{"name":"job_status","arguments":{"job_id":"job-RETURNED_ID"}}
{"name":"search","arguments":{"query":"Proxy::all","mode":"hybrid","source_id":"reqwest","context":{"max_total_chars":12000}}}
{"name":"get_document","arguments":{"source_id":"reqwest","url":"RETURNED_DOCUMENT_URL"}}
{"name":"get_section","arguments":{"source_id":"reqwest","url":"RETURNED_DOCUMENT_URL","section_id":3}}
{"name":"recrawl","arguments":{"source_id":"reqwest","max_pages":100}}
```

## Job lifecycle

Admission persists a `queued` job and returns promptly. A supervisor transitions
it to `running`, then `completed`, `partial`, `failed`, or `cancelled`. There are
at most two admitted jobs per server, one active job per source, two HTTP requests
per job, and a five-minute execution deadline. Busy admission fails for retry;
there is no unbounded waiting queue. Four foreground MCP calls and one
vector/hybrid search can execute concurrently. Foreground calls retain the
180-second deadline and RPC cancellation behavior.

Accepted background jobs outlive the crawl tool call. RPC cancellation during
admission cannot leave a durable row without a worker. Use `source_status` to
recover the ID if a client loses an admission response. Use `cancel_job` to stop
a job. Cancelled tasks release their slots after cleanup, and existing committed
knowledge remains searchable. Cancellation cannot roll back a transaction that
already committed; a cancelled job may therefore have a completed text crawl.

`job_status.run.ingestion` reports stage checkpoints: crawl, prepare, persist,
embeddings, complete. It is not a live count of URLs still in Spider's queue.
During admission the run can be absent. A completed text crawl can still have a
partial embedding stage. The resolved model, embedding coverage and generation
counts remain auditable. Source-wide embedding retries can use the existing CLI
`embed` after stopping MCP.

EOF and Ctrl-C cancel active jobs and wait for bounded worker cleanup. On startup,
queued/running jobs left by an interrupted process become `interrupted`; running
crawl and ingestion records are recovered too. Completed records are preserved.
Jobs are never automatically retried or resumed after a crash: inspect status and
submit a new `recrawl` with a new job ID.

## Structured discovery

Pass `discover:true` to MCP crawl/recrawl, or `--discover` to CLI ingest/recrawl.
For a documentation subtree ending in `/`, Ariadne probes `llms.txt`,
`llms-full.txt` and `sitemap.xml` under that subtree. Files outside its registered
scope are skipped. The pass reads at most six manifests, 512 KiB per manifest,
1000 extracted URLs and no more seed URLs than the page budget. One-page crawls
skip discovery. Manifest requests and seeds respect Spider's robots decisions and
crawl delay. Manifest redirects are refused. XML DTDs are refused; plain XML
sitemap indexes and namespaced URL sets are supported. Gzip sitemaps and arbitrary
manifest content ingestion are deferred.

Markdown links and sitemap locations seed Spider's existing frontier. Pages are
fetched and extracted separately under the existing page/depth/redirect budgets;
manifest text is never substituted for a page's contents or provenance. Normal
HTML navigation/internal links remain Spider's responsibility. Manifest failures,
byte limits and parse outcomes appear in `run.summary.discovery`; they do not
by themselves make otherwise successful page ingestion fail. This discovery pass
does not prove complete site coverage and never authorizes deleting unseen pages.

## Measurements and recovery validation

The local two-source fixture measured roughly 7 ms per admission on 2026-10-07;
this is a development-machine observation, not a service latency guarantee.
`job.elapsed_ms` measures admission through terminal completion. Run summaries
include `crawl_elapsed_ms`, `retained_body_bytes` for retained page audit bodies,
and bounded manifest attempts with retained byte counts. Existing page/chunk,
rejection, changed/unchanged and embedding counters remain available. Retained
bytes are not total network traffic or peak resident memory. These explicit limits
are an initial resource policy, not a production throughput benchmark.

`cargo test --test agent_operations -- --nocapture` measures admission latency and
checks two independently scoped sources through the official MCP client, full
embedding/indexing, hybrid search, context, bounded document reads, recrawl reuse,
source pagination and durable status. The cancellation fixture holds HTTP open
and proves admission returns before fetching finishes. Unit tests check public
address/DNS policy, schema upgrade, interrupted jobs and closed-store backup
restoration. Existing crawler fixtures cover redirect and scope boundaries.
See [storage recovery guidance](storage.md#backup-and-restore).

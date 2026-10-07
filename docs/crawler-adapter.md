# Crawler adapter

The `ariadne::crawler` library API wraps pinned Spider 2.53.9. It reuses Spider's
frontier, HTTP fetching, concurrency, robots handling, and link discovery.
Ingestion, persistence, extraction, and opt-in guarded Chromium rendering are
provided by surrounding Ariadne modules. See [browser fallback](browser-fallback.md).

```rust
use ariadne::crawler::{crawl, CrawlRequest, CrawlScope, PageState};

// Inside an async application:
let seed = "https://example.com/docs/".parse()?;
let scope = CrawlScope::new("https://example.com".parse()?, "/docs")?;
let request = CrawlRequest::new("source-id", "crawl-run-id", seed, scope);
let report = crawl(request).await?;
if !report.delivery_complete() {
    // Persist an incomplete run and arrange recovery; never treat it as a
    // complete source snapshot or use it to remove missing documents.
}
for page in report.pages {
    if page.state == PageState::Fetched {
        // Pass raw bytes plus provenance to extraction.
    } else {
        // Persist a failed/blocked page outcome without indexing its error body.
    }
}
```

## Scope and redirects

- Scope includes scheme, hostname, effective port, and a path-component prefix.
  `/docs` accepts `/docs/api` and excludes `/docs-old`.
- Seeds must be in scope, use HTTP(S), and omit URL credentials.
- Discovery uses an escaped, anchored whitelist. Encoded paths are conservatively
  rejected in this first slice; queries remain intact and fetch fragments are removed.
- Every redirect is checked against origin, path, path-depth, and redirect count
  before following it. Multi-hop escapes and loops produce bounded audit records.
- The policy composes Spider's strict redirect/internal-address policy. Local
  redirects require an explicit `allow_loopback_redirects` opt-in and remain
  confined to the exact seed origin and path scope. The exception only accepts
  literal IPv4/IPv6 loopback seeds.
- Environment proxies are disabled for this initial adapter. Proxy configuration
  needs an explicit future interface and its own scope/access tests.
- Spider may request `/robots.txt` outside the document path prefix as an ancillary
  request. That response is not an indexed document.

This is an application crawl scope, not a complete network isolation mechanism.
DNS-address enforcement/rebinding protection and remotely supplied URL access
policies still need validation before exposing crawl operations to remote clients.

## Outcomes and limits

Each page outcome carries source/run IDs, requested/final URLs, capture time,
HTTP status, lossless header values, raw bytes, truncation metadata, and a typed
state. HTTP errors, transport errors, blocked pages, truncated responses, and
oversized retained bodies are not successful extraction inputs.

Defaults: 100 pages, concurrency two, a 20-second request timeout, five redirects,
robots enabled, and 2 MiB of retained body data per outcome. Automatic retries
are disabled pending verification of backoff and `Retry-After` behavior.

`max_path_segments` is an absolute path segment cap. It is applied to redirects
and Spider's discovery budget without the engine's seed-relative depth adjustment.
There is no link-hop limit in this API yet.

Reports retain at most `max_pages` page outcomes and `audit_capacity` blocked
records. A broadcast lag error, excess page emission, or full audit channel makes
`delivery_complete()` false; partial results remain inspectable. The collector
runs alongside the crawler without detached collector tasks. Results are sorted
by source URL after collection; a budgeted crawl's selected URLs can still depend
on concurrent scheduling.

The body limit controls retained data only. The pinned engine's per-page byte
setting is browser-specific; HTTP fetching has a separate process-wide engine
limit. A per-request HTTP download limit and bounded streaming ingestion remain
future work. Also pending: cancellation with verified task cleanup, whole-run
deadlines, retry policies, complete skipped-link auditing, and link-hop traversal.
Request timeouts currently bound individual requests rather than the entire run.

Delivery completeness is not source completeness: page budgets, depth limits,
robots rules, failures, and discovery gaps can all leave a source partially visited.

## Verification

`cargo test` exercises deterministic crawler fixtures covering redirects, scope,
robots, budgets, metadata, and failure paths. The ignored Chromium acceptance
tests use a local fixture and run with
`cargo test --test browser_fallback -- --ignored`. Browser behavior on public
sites remains site dependent.

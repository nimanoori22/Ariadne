# Browser fallback for dynamic documentation

Ariadne fetches documentation with Spider's HTTP crawl path. With browser fallback
enabled, a page whose HTML looks like an empty JavaScript application shell is
rendered in local Chromium. Static pages stay on the HTTP path. Spider still owns
the crawl frontier, robots policy, concurrency, and link discovery; links added
to the rendered DOM enter the same scoped crawl.

Enable it for foreground ingestion or recrawling with `--browser-fallback`:

```text
Ariadne ingest <source-id> <crawl-id> --browser-fallback
Ariadne recrawl <source-id> <crawl-id> --browser-fallback
```

For MCP jobs, the process owner sets `ARIADNE_BROWSER_FALLBACK=1` before starting
the server. The MCP caller cannot enable it per request. Set
`ARIADNE_CHROMIUM_PATH` if Chromium is not found in `PATH`. Rendering is disabled
by default. Local Chromium must be installed with a working sandbox.

Chromium starts lazily with a temporary profile. Its requests pass through a
local rejecting proxy. Ariadne intercepts document, script, stylesheet, and
fetch/XHR requests and serves allowed resources through the crawler's guarded
HTTP client. Only GET requests within the source's origin/path scope and robots
policy are eligible. Other resource types, extra navigation, and over-budget
requests are rejected. The browser has no persistent profile or user cookies.
Resource redirects return to interception so each destination is checked before
fetching, including destinations disallowed by robots.txt.
The browser closes after the crawl. This is a bounded rendering path for trusted
documentation sources, not a general-purpose browser automation service.

Each page retains its HTTP status, headers, and provenance. A rendered page stores
the original HTTP body in `page.rendering.original_body` and the rendered DOM as
`page.raw_body`. The audit also records render outcome, elapsed time, resource
count and bytes, blocked URLs, and requests rejected by the proxy. The page body
limit and request timeout apply to rendering. If rendering fails or times out,
the page is recorded as `RenderFailure` and is not indexed as a loading shell.
Existing successfully indexed content is preserved by normal partial-crawl
rules. A rendered page is fetched and rendered again on recrawl even when its
initial HTML and ETag are unchanged, so changed JavaScript data is not missed.

The heuristic is intentionally conservative: it targets script-backed pages with
little meaningful static content or loading placeholders. A site with substantial
static prose and additional important JavaScript content may stay on the HTTP
path. Client-side navigation after the initial rendered state, authentication,
WebSockets, service workers, and arbitrary cross-origin assets are outside this
first fallback. For such sources, use a more suitable source representation or
extend the rendering policy with fixture-backed requirements.

`cargo test --test browser_fallback -- --ignored` runs two real Chromium tests
against a local fixture. They verify rendered content and links, scope/robots
protection, incremental recrawl with unchanged static HTML, and timeout auditing.

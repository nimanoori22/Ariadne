# First crawler integration probe

Run `cargo test --test spider_capabilities`.

The probe pins Spider 2.53.9 as a development dependency with `sync`, `regex`,
`headers`, and `page_error_status_details`;
The engine probes remain separate from the [static crawler adapter](crawler-adapter.md).
The local HTTP fixture
binds an ephemeral loopback port and requires no public website, browser, database,
or embedding credentials. Initial dependency downloads require registry access.

## Verified by the eight integration tests

- Internal relative links are discovered; duplicate fragment links yield one API page.
- A configured whitelist prevents fetching the fixture's direct out-of-path link.
- Raw HTML retains heading anchors, code indentation, and tables.
- A one-page budget fetches only the seed on this fixture.
- Subscription closure allows consumers to finish.
- An idle consumer with capacity one receives an explicit lag error. Broadcast
  delivery therefore must not be treated as a lossless ingestion queue.
- The default policy refuses the fixture's redirect to a loopback destination.
  The target is not fetched, consistent with the inspected redirect SSRF guard.
- With a test-only HTTP client permitting local redirects, an allowed redirect
  delivers the target content, success status, original URL, and final destination.
- With that same permissive client, a discovered-URL whitelist does not stop a
  redirect outside the path or to another hostname. The fixture changes
  `127.0.0.1` to `localhost`; all traffic still stays on the fixture server.
  This characterizes custom-client behavior, not successful public redirects
  through Spider's default policy.
- `with_depth(2)` on the fixture seed `/docs/depth` excludes `/docs/depth/child`
  but permits a three-hop link chain whose URLs each have two path segments.
  Increasing the limit to three permits the nested child. Depth is based on
  URL path segments here, not link distance from the seed.
- Enabling robots handling fetches `/robots.txt`, permits the allowed page,
  and prevents the disallowed page from being requested or delivered. Disabling
  it permits the disallowed page as a control.
- HTTP 404 and 503 outcomes reach subscribers with their URL and status; the 503
  retains its `Retry-After` header. With configured retries zero, each of these
  URLs is requested once. A connection closed without a response is delivered
  with a nonempty transport error.

## Requirements for the future adapter

- Apply scope to every redirect hop before it is fetched. Discovery whitelist
  checks alone are insufficient. Preserve the internal-address guard when using
  a custom client; the permissive test policy is not suitable for production.
- Define path-depth and link-hop limits separately. Do not present Spider's
  `with_depth` setting as a BFS hop limit without additional implementation.
- Enable robots handling explicitly and retain blocked decisions in crawl audit
  records. The fixture verifies wildcard disallow behavior, not the full robots
  standard or crawl-delay handling.
- Map HTTP status, response headers, requested/final URLs, and transport errors
  into explicit page outcomes. Treat failure pages as failures rather than
  extracting and indexing their error bodies as documentation.
- Treat subscription lag as an explicit incomplete run until a verified lossless
  delivery or recovery strategy is implemented.

## Remaining validation

1. Validate public-address and DNS behavior. The adapter now has separate tests
   for its redirect policy, hop caps, loops, origin boundaries, and multi-hop
   escapes. The original engine success/escape probes substitute a permissive client.
2. Test retry/backoff policies, `Retry-After` enforcement, request timeouts,
   cancellation, robots user-agent selection, and blocked seed URLs.
3. Test sustained slow ingestion. Select and validate a bounded delivery strategy
   that accounts for every page or explicitly fails the run on lag.
4. Evaluate `spider_transformations` against documentation fixtures for preservation
   of code, tables, and links. Define a structured document model independently
   of the Markdown output.
5. Add browser-only fixtures when static crawling and extraction are established.

These tests are narrow capability checks, not a claim that arbitrary websites or
all crawl boundaries are supported. SurrealDB and MCP feasibility probes remain
part of roadmap step 1.

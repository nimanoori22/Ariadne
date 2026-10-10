# Install Ariadne and crawl a full HTML source

Install the lowercase executable from the repository:

```sh
cargo install --path . --locked --bin ariadne --root "$HOME/.local"
ariadne --help
```

Add `$HOME/.local/bin` to your PATH if needed. The original `Ariadne` development
binary remains available. The database opens and migrates automatically in the
platform application-data directory. On Linux this normally means
`~/.local/share/ariadne/knowledge`. Set `ARIADNE_DATA_DIR` to choose another
durable directory; use the same setting for CLI and MCP.

```sh
ariadne source add boj-en 'Bank of Japan English' https://www.boj.or.jp/en/
ariadne crawl-site boj-en boj-en-full --discover --concurrency 2
ariadne site-status boj-en boj-en-full
ariadne search 'monetary policy' --source boj-en
```

`crawl-site` has no total page limit. Spider fetches bounded batches of 20 URLs
with the configured concurrency. A SurrealDB frontier records pending, fetched,
failed and robots-blocked URLs; each batch commits documents, chunks, audit
outcomes, coverage and newly discovered links atomically. HTML navigation links
are discovered before content extraction removes navigation. `--discover` also
uses the existing bounded manifest-discovery pipeline at the source root.

The crawl stops when its reachable URL queue is exhausted. A completed queue
can still contain failures or rejected documents, so inspect coverage. The CLI
returns an error for incomplete coverage. This is static HTML indexing: PDFs,
spreadsheets, JavaScript-only content and pages with no discoverable links are
outside this mode. The existing robots, redirect, URL scope and content-size
guards still apply. A source rooted at `/en/` stays on that origin and path;
the path is a scope rule, not language detection.

URL scheduling admits extensionless paths and common HTML extensions (`htm`,
`html`, `xhtml`, `shtml`, `php`, `asp`, `aspx`, `jsp`). Other extensions are
excluded before fetching; older queued file URLs are marked `skipped` on resume.
The extractor still checks response format, including extensionless downloads.

After a process interruption, reopen the database and resume the same run:

```sh
ariadne crawl-site boj-en boj-en-full --resume --discover --concurrency 2
```

Committed URLs remain complete and the pending batch is retried. Resume accepts
interrupted, failed or cancelled runs. To retry failures from a completed run,
start a new crawl ID. Text search works without Ollama; run `ariadne embed boj-en`
separately if you want semantic retrieval.

## Codex

Run one shared HTTP MCP server for all Codex sessions. The user service keeps
one process owning the embedded datastore:

```sh
mkdir -p "$HOME/.config/systemd/user" "$HOME/.config/ariadne"
cp contrib/systemd/ariadne-mcp.service "$HOME/.config/systemd/user/"
printf '%s\n' 'ARIADNE_MCP_CRAWL_SOURCES=boj-en' > "$HOME/.config/ariadne/mcp.env"
systemctl --user daemon-reload
systemctl --user enable --now ariadne-mcp.service
codex mcp remove ariadne
codex mcp add ariadne --url http://127.0.0.1:3847/mcp
codex mcp list
```

Stop existing stdio Ariadne processes before starting the service. The env file
belongs to the server; Codex HTTP entries do not pass environment variables.
Preserve other embedding and crawl settings there if configured. Open a new
Codex session after updating its connection. This follows the
[official Codex MCP setup](https://developers.openai.com/codex/mcp/).
The source allowlist grants crawl access only to `boj-en`; the normal public
network validation remains enabled.

For foreground use, run `ariadne mcp-http` (default `127.0.0.1:3847`) or supply
another loopback socket address. The endpoint is `/mcp`. It rejects non-loopback
bindings, untrusted Host headers and browser Origin headers. It has no remote
authentication and is intended for local agents. Clients share request limits,
embedding initialization and crawl jobs; disconnecting a client leaves jobs
running. SIGINT and SIGTERM stop admission and cancel jobs during shutdown.

Inspect the service with `systemctl --user status ariadne-mcp` and
`journalctl --user -u ariadne-mcp`. Restart it after replacing the binary or
changing `mcp.env`. `ariadne mcp` remains available for a single stdio client.

The server exposes `search`, `get_document`, `list_sources`, `source_status` and
the other existing knowledge tools. Its new `crawl_site` tool accepts
`{"source_id":"boj-en","discover":true}` and promptly returns a supervised
job ID. Use `job_status` for persisted coverage and `cancel_job` to stop it.
Full-site jobs have no total page limit or five-minute deadline. Search remains
available on committed batches while this job runs. These crawls index HTML
lexically; embeddings remain a separate operation.

Only one process can own an embedded database directory. Use MCP tools for
operations while the shared server runs. For CLI maintenance, run
`systemctl --user stop ariadne-mcp`, execute the CLI commands, then
`systemctl --user start ariadne-mcp`. Never remove the database LOCK file to
work around a live owner. Separate HTTP clients share the server; a separate
CLI process cannot share its open datastore.

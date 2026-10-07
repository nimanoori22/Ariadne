use anyhow::{Context, Result, bail, ensure};
use ariadne::{
    crawler::{CrawlRequest, CrawlScope, crawl},
    embeddings::{OllamaConfig, OllamaProvider, index_source},
    ingestion::{
        IngestionStatus, extract_crawl, ingest, ingest_lexical, recrawl, recrawl_lexical,
        reprocess_lexical,
    },
    retrieval::{
        ContextOptions, SearchMode, SearchQuery, assemble_context, hybrid_search, search,
        vector_search,
    },
    storage::{KnowledgeStore, Source},
};
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if matches!(args.first().map(String::as_str), Some("--help" | "help")) {
        println!(
            "Ariadne\n  mcp\n  ingest <source-id> <crawl-id> [--lexical-only] [--discover] [--max-pages <n>] [--concurrency <n>] [--chunk-chars <n>]\n  recrawl <source-id> <crawl-id> [--lexical-only] [--discover] [--max-pages <n>] [--concurrency <n>] [--chunk-chars <n>]\n  reprocess <source-id> <crawl-id> [--max-pages <n>] [--chunk-chars <n>]\n  source status <source-id>\n  document-status <source-id> <url>\n  source add <id> <name> <url>\n  source list\n  crawl <source-id> <crawl-id>\n  run <source-id> <crawl-id>\n  document <source-id> <url>\n  chunks <source-id> <url>\n  indexing <source-id> <url>\n  search <query> [--source <id>] [--limit <n>] [--mode auto|keywords|exact] [--max-chars <n>]\n  embed <source-id>\n  embeddings spaces\n  embeddings status <source-id>\n  hybrid-search <query> [--source <id>] [--limit <n>]\n  retrieve <query> [--retrieval-mode lexical|vector|hybrid] [--source <id>] [--limit <n>] [--neighbors <n>] [--context-chars <n>]\n  Search filters: --url-prefix <url> --heading <heading> --crawled-after <unix-seconds>\n  vector-search <query> [--source <id>] [--limit <n>] [--max-chars <n>]\n\nThe local database is opened and migrated automatically.\nSet ARIADNE_DATA_DIR to override the application data directory.\nEmbeddings use local Ollama: ARIADNE_OLLAMA_URL and ARIADNE_EMBED_MODEL (default embeddinggemma:latest)."
        );
        return Ok(());
    }
    let store = KnowledgeStore::open_default().await?;
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["mcp"] => ariadne::mcp::serve_stdio(store, OllamaConfig::from_env()?).await?,
        [
            operation @ ("ingest" | "recrawl" | "reprocess"),
            source_id,
            crawl_id,
            options @ ..,
        ] => {
            let source = store
                .get_source(source_id)
                .await?
                .context("source not found; use source add")?;
            let scope = CrawlScope::new(source.root_url.clone(), source.root_url.path())?;
            let mut request =
                CrawlRequest::new(*source_id, *crawl_id, source.root_url.clone(), scope);
            let mut lexical_only = false;
            let mut policy = ariadne::chunking::ChunkPolicy::default();
            let mut cursor = 0;
            while cursor < options.len() {
                match options[cursor] {
                    "--discover" => {
                        ensure!(
                            *operation != "reprocess",
                            "discovery applies to network ingestion only"
                        );
                        request.discovery = true;
                        cursor += 1;
                    }
                    "--lexical-only" => {
                        lexical_only = true;
                        cursor += 1;
                    }
                    "--max-pages" | "--concurrency" | "--chunk-chars" => {
                        let value = options
                            .get(cursor + 1)
                            .context("ingest option requires a value")?;
                        if options[cursor] == "--max-pages" {
                            request.max_pages = value.parse()?;
                        } else if options[cursor] == "--concurrency" {
                            request.concurrency = value.parse()?;
                        } else {
                            policy.target_chars = value.parse()?;
                        }
                        cursor += 2;
                    }
                    _ => bail!("unknown ingest option; use --help"),
                }
            }
            let result = if *operation == "reprocess" {
                reprocess_lexical(&store, source, request, policy).await
            } else if lexical_only {
                if *operation == "recrawl" {
                    recrawl_lexical(&store, source, request, policy).await
                } else {
                    ingest_lexical(&store, source, request, policy).await
                }
            } else {
                let config = OllamaConfig::from_env()?;
                if *operation == "recrawl" {
                    recrawl(&store, source, request, policy, || {
                        OllamaProvider::connect(config)
                    })
                    .await
                } else {
                    ingest(&store, source, request, policy, || {
                        OllamaProvider::connect(config)
                    })
                    .await
                }
            };
            let run = store.get_crawl(source_id, crawl_id).await?;
            print_json(&run)?;
            result?;
            if run
                .and_then(|r| r.ingestion)
                .is_some_and(|p| p.status != IngestionStatus::Completed)
            {
                bail!(
                    "ingestion is partial; inspect run and embeddings status; retry embeddings with embed"
                );
            }
        }
        [] => println!("Ariadne database ready: {}", store.path().display()),
        ["search", query, options @ ..] => {
            print_json(&search(&store, search_query(query, options)?).await?)?
        }
        ["hybrid-search", query, options @ ..] => {
            let request = search_query(query, options)?;
            request.validate()?;
            let provider = OllamaProvider::connect(OllamaConfig::from_env()?).await?;
            print_json(&hybrid_search(&store, &provider, request).await?)?;
        }
        ["retrieve", query, options @ ..] => {
            let (request, mode, context) = retrieval_options(query, options)?;
            let hits = if mode == "lexical" {
                search(&store, request).await?
            } else {
                let provider = OllamaProvider::connect(OllamaConfig::from_env()?).await?;
                if mode == "hybrid" {
                    hybrid_search(&store, &provider, request).await?
                } else {
                    vector_search(&store, &provider, request).await?
                }
            };
            print_json(&assemble_context(&store, &hits, context).await?)?;
        }
        ["embed", source] => {
            store
                .get_source(source)
                .await?
                .context("source not found")?;
            let provider = OllamaProvider::connect(OllamaConfig::from_env()?).await?;
            let report = index_source(&store, &provider, source).await?;
            print_json(&report)?;
            if report.failed > 0 {
                bail!(
                    "{} chunks failed embedding; inspect embeddings status and rerun embed",
                    report.failed
                );
            }
        }
        ["embeddings", "spaces"] => print_json(&store.list_embedding_spaces().await?)?,
        ["embeddings", "status", source] => {
            store
                .get_source(source)
                .await?
                .context("source not found")?;
            let mut states = Vec::new();
            for space in store.list_embedding_spaces().await? {
                states.push(serde_json::json!({"space": space, "coverage": store.embedding_coverage(&space, source).await?, "states": store.embedding_states(&space, source).await?}));
            }
            print_json(&states)?;
        }
        ["vector-search", query, options @ ..] => {
            let request = search_query(query, options)?;
            request.validate()?;
            if request.mode != SearchMode::Auto {
                bail!("lexical match modes do not apply to vector search");
            }
            let provider = OllamaProvider::connect(OllamaConfig::from_env()?).await?;
            print_json(&vector_search(&store, &provider, request).await?)?;
        }
        ["source", "add", id, name, url] => print_json(
            &store
                .register_source(Source::new(*id, *name, Url::parse(url)?)?)
                .await?,
        )?,
        ["source", "list"] => print_json(&store.list_sources().await?)?,
        ["source", "status", source] => print_json(&store.source_status(source).await?)?,
        ["document-status", source, url] => {
            print_json(&store.document_status(source, &Url::parse(url)?).await?)?
        }
        ["chunks", source, url] => print_json(&store.get_chunks(source, &Url::parse(url)?).await?)?,
        ["indexing", source, url] => print_json(
            &store
                .get_indexing(source, &Url::parse(url)?)
                .await?
                .context("document has not been indexed")?,
        )?,
        ["run", source, run] => print_json(
            &store
                .get_crawl(source, run)
                .await?
                .context("crawl not found")?,
        )?,
        ["document", source, url] => print_json(
            &store
                .get_document(source, &Url::parse(url)?)
                .await?
                .context("document not found")?,
        )?,
        ["crawl", source_id, crawl_id] => {
            let source = store
                .get_source(source_id)
                .await?
                .context("source not found")?;
            let scope = CrawlScope::new(source.root_url.clone(), source.root_url.path())?;
            let request = CrawlRequest::new(*source_id, *crawl_id, source.root_url, scope);
            store.begin_crawl(&request).await?;
            let result = async {
                let report = crawl(request).await?;
                store.finish_crawl(extract_crawl(report)).await
            }
            .await;
            if let Err(error) = result {
                store
                    .fail_crawl(source_id, crawl_id, &format!("{error:#}"))
                    .await
                    .context("record crawl failure")?;
                return Err(error);
            }
            print_json(&store.get_crawl(source_id, crawl_id).await?)?;
        }
        _ => bail!("invalid arguments; use --help"),
    }
    Ok(())
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn search_query(query: &str, options: &[&str]) -> Result<SearchQuery> {
    let mut request = SearchQuery::new(query);
    let (pairs, remainder) = options.as_chunks::<2>();
    if !remainder.is_empty() {
        bail!("search options require a flag and value");
    }
    for pair in pairs {
        match pair {
            ["--url-prefix", prefix] => request.filter.url_prefix = Some((*prefix).into()),
            ["--heading", heading] => request.filter.heading = Some((*heading).into()),
            ["--crawled-after", time] => {
                request.filter.crawled_after =
                    Some(time.parse().context("invalid crawl timestamp")?)
            }
            ["--source", source] => request.source_id = Some((*source).to_owned()),
            ["--limit", limit] => request.limit = limit.parse().context("invalid search limit")?,
            ["--max-chars", budget] => {
                request.max_text_chars = budget.parse().context("invalid search text budget")?
            }
            ["--mode", "auto"] => request.mode = SearchMode::Auto,
            ["--mode", "keywords"] => request.mode = SearchMode::Keywords,
            ["--mode", "exact"] => request.mode = SearchMode::Exact,
            _ => bail!("unknown search option; use --help"),
        }
    }
    Ok(request)
}

fn retrieval_options<'a>(
    query: &str,
    options: &'a [&str],
) -> Result<(SearchQuery, &'a str, ContextOptions)> {
    let mut context = ContextOptions::default();
    let mut mode = "hybrid";
    let mut search_options = Vec::new();
    let (pairs, remainder) = options.as_chunks::<2>();
    ensure!(
        remainder.is_empty(),
        "retrieve options require a flag and value"
    );
    for pair in pairs {
        match pair {
            ["--retrieval-mode", value] => {
                ensure!(
                    matches!(*value, "lexical" | "vector" | "hybrid"),
                    "invalid retrieval mode"
                );
                mode = value;
            }
            ["--neighbors", value] => context.neighbor_chunks = value.parse()?,
            ["--context-chars", value] => context.max_total_chars = value.parse()?,
            ["--context-chunks", value] => context.max_chunks = value.parse()?,
            ["--context-chunk-chars", value] => context.max_chunk_chars = value.parse()?,
            _ => search_options.extend_from_slice(pair),
        }
    }
    let request = search_query(query, &search_options)?;
    request.validate()?;
    context.validate()?;
    Ok((request, mode, context))
}

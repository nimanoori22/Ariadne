//! Embedded SurrealDB and the sole SurrealQL boundary.
mod embeddings;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::{Context, Result, ensure};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use surrealdb::{
    Surreal,
    engine::any::{Any, connect},
    opt::{Config, capabilities::Capabilities},
};
use surrealkit::{EmbeddedSchemaFile, Sync};
use url::Url;

use crate::{
    chunking::{Chunk, ChunkPolicy, IndexMetadata},
    crawler::{CrawlRequest, CrawlScope},
    extraction::{ExtractedDocument, ExtractionOutcome},
    ingestion::{ExtractionBatch, PreparedCrawl, prepare_crawl},
};

static SCHEMA: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
    path: "database/schema/knowledge.surql",
    sql: include_str!("../../database/schema/knowledge.surql"),
}];

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub root_url: Url,
    pub created_at: SystemTime,
}

impl Source {
    pub fn new(id: impl Into<String>, name: impl Into<String>, mut root_url: Url) -> Result<Self> {
        root_url.set_fragment(None);
        let source = Self {
            id: id.into(),
            name: name.into(),
            root_url,
            created_at: SystemTime::now(),
        };
        source.validate()?;
        Ok(source)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.id.trim().is_empty() && !self.name.trim().is_empty(),
            "source identity and name must be nonempty"
        );
        CrawlScope::new(self.root_url.clone(), self.root_url.path())?;
        ensure!(
            self.root_url.fragment().is_none(),
            "source root must not contain a fragment"
        );
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrawlRun {
    pub source_id: String,
    pub crawl_id: String,
    pub status: String,
    pub started_at: SystemTime,
    pub finished_at: Option<SystemTime>,
    /// Validated request snapshot, including scope and resource budgets.
    pub configuration: Value,
    pub summary: Option<Value>,
    pub error: Option<String>,
    #[serde(default)]
    pub ingestion: Option<crate::ingestion::IngestionProgress>,
}

/// One embedded engine per application. The SDK remains private: callers use
/// knowledge operations rather than database queries.
pub struct KnowledgeStore {
    db: Surreal<Any>,
    path: PathBuf,
}

impl KnowledgeStore {
    /// Internal persistence operation. Public callers use retrieval::search so
    /// request bounds and exact-match policy are applied consistently.
    pub(crate) async fn lexical_search(
        &self,
        query: &str,
        source_id: Option<&str>,
        limit: usize,
        max_text_chars: usize,
        exact_pattern: &str,
    ) -> Result<Vec<crate::retrieval::KnowledgeHit>> {
        let mut result = self
            .db
            .query("BEGIN TRANSACTION;")
            .query(format!(
                "{} {}",
                include_str!("search_projection.surql"),
                include_str!("search_text.surql")
            ))
            .query(format!(
                "{} {}",
                include_str!("search_projection.surql"),
                include_str!("search_title.surql")
            ))
            .query("COMMIT TRANSACTION;")
            .bind(("query", query.to_owned()))
            .bind(("source_id", source_id.unwrap_or_default().to_owned()))
            .bind(("limit", limit))
            .bind(("max_text_chars", max_text_chars))
            .bind(("exact_pattern", exact_pattern.to_owned()))
            .await
            .context("execute lexical search")?
            .check()
            .context("evaluate lexical search")?;
        let mut values: Vec<Value> = result.take(1)?;
        values.extend(result.take::<Vec<Value>>(2)?);
        values
            .into_iter()
            .map(|value| serde_json::from_value(value).map_err(Into::into))
            .collect()
    }
    pub fn default_path() -> Result<PathBuf> {
        if let Some(path) = std::env::var_os("ARIADNE_DATA_DIR") {
            ensure!(!path.is_empty(), "ARIADNE_DATA_DIR must not be empty");
            return Ok(PathBuf::from(path).join("knowledge"));
        }
        let dirs = ProjectDirs::from("", "", "ariadne")
            .context("cannot determine application data directory; set ARIADNE_DATA_DIR")?;
        Ok(dirs.data_local_dir().join("knowledge"))
    }

    pub async fn open_default() -> Result<Self> {
        Self::open(Self::default_path()?).await
    }

    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref()).context("create Ariadne database directory")?;
        let path = std::fs::canonicalize(path).context("resolve Ariadne database directory")?;
        let text = path.to_str().context("database path must be valid UTF-8")?;
        ensure!(
            !text.contains(['?', '#']),
            "database path must not contain ? or #"
        );
        let config = Config::new().capabilities(
            Capabilities::default()
                .with_all_functions_allowed()
                .with_all_net_targets_denied(),
        );
        let db = connect((format!("surrealkv://{text}"), config))
            .await
            .context("open embedded SurrealKV; another process may already hold this directory")?;
        db.use_ns("ariadne").use_db("knowledge").await?;
        // Schema SQL ships in the binary. Removed definitions never trigger
        // automatic deletion; destructive/data migrations need explicit rollouts.
        Sync::embedded(SCHEMA)
            .prune(false)
            .run(&db)
            .await
            .context("apply embedded SurrealKit schema")?;
        // An exclusive datastore is owned by this process. Running jobs left by
        // its previous owner are interrupted, never silently reported successful.
        db.query("UPDATE crawl_run SET status = 'interrupted', data.status = 'interrupted', data.error = 'application stopped before crawl completion' WHERE status = 'running';").await?.check()?;
        db.query("UPDATE crawl_run SET data.ingestion.status = 'interrupted', data.ingestion.error = 'application stopped before ingestion completion', data.ingestion.updated_at = $now, data.ingestion.finished_at = $now WHERE data.ingestion.status = 'running';")
            .bind(("now", serde_json::to_value(SystemTime::now())?)).await?.check()?;
        tracing::info!(path = %path.display(), "opened embedded knowledge store");
        let store = Self { db, path };
        store.restore_embedding_schemas().await?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Exact duplicate registration returns the original timestamps. Conflicting
    /// source IDs fail instead of changing the meaning of existing provenance.
    pub async fn register_source(&self, source: Source) -> Result<Source> {
        source.validate()?;
        self.db
            .query(include_str!("register_source.surql"))
            .bind(("source_id", source.id.clone()))
            .bind(("data", serde_json::to_value(&source)?))
            .await?
            .check()?;
        self.get_source(&source.id)
            .await?
            .context("registered source missing")
    }

    pub async fn get_source(&self, id: &str) -> Result<Option<Source>> {
        let mut result = self
            .db
            .query("SELECT VALUE data FROM type::record('source', $id);")
            .bind(("id", id.to_owned()))
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .next()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn list_sources(&self) -> Result<Vec<Source>> {
        let mut result = self
            .db
            .query("SELECT VALUE data FROM source ORDER BY data.id;")
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Into::into))
            .collect()
    }

    pub async fn begin_crawl(&self, request: &CrawlRequest) -> Result<()> {
        request.validate()?;
        let run = CrawlRun {
            source_id: request.source_id.clone(),
            crawl_id: request.crawl_id.clone(),
            status: "running".into(),
            started_at: SystemTime::now(),
            finished_at: None,
            configuration: serde_json::to_value(request)?,
            summary: None,
            error: None,
            ingestion: None,
        };
        self.db.query("BEGIN TRANSACTION; IF !record::exists(type::record('source', $source_id)) { THROW 'unknown source'; }; CREATE ONLY type::record('crawl_run', [$source_id, $crawl_id]) SET source = type::record('source', $source_id), status = 'running', data = $data; COMMIT TRANSACTION;")
            .bind(("source_id", request.source_id.clone())).bind(("crawl_id", request.crawl_id.clone())).bind(("data", serde_json::to_value(run)?)).await?.check()?;
        Ok(())
    }

    pub async fn get_crawl(&self, source_id: &str, crawl_id: &str) -> Result<Option<CrawlRun>> {
        let mut result = self
            .db
            .query("SELECT VALUE data FROM type::record('crawl_run', [$source_id, $crawl_id]);")
            .bind(("source_id", source_id.to_owned()))
            .bind(("crawl_id", crawl_id.to_owned()))
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .next()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn fail_crawl(&self, source_id: &str, crawl_id: &str, error: &str) -> Result<()> {
        self.db.query("BEGIN TRANSACTION; LET $run = type::record('crawl_run', [$source_id, $crawl_id]); IF $run.status != 'running' { THROW 'crawl is not running'; }; UPDATE $run SET status = 'failed', data.status = 'failed', data.error = $error, data.finished_at = $finished; COMMIT TRANSACTION;")
            .bind(("source_id", source_id.to_owned())).bind(("crawl_id", crawl_id.to_owned())).bind(("error", error.to_owned())).bind(("finished", serde_json::to_value(SystemTime::now())?)).await?.check()?;
        Ok(())
    }

    pub(crate) async fn set_ingestion_progress(
        &self,
        source_id: &str,
        crawl_id: &str,
        progress: &crate::ingestion::IngestionProgress,
    ) -> Result<()> {
        self.db.query("BEGIN TRANSACTION; LET $run = type::record('crawl_run', [$source_id, $crawl_id]); IF !record::exists($run) { THROW 'unknown crawl'; }; UPDATE $run SET data.ingestion = $progress; COMMIT TRANSACTION;")
            .bind(("source_id", source_id.to_owned())).bind(("crawl_id", crawl_id.to_owned()))
            .bind(("progress", serde_json::to_value(progress)?)).await?.check()?;
        Ok(())
    }

    /// A bounded crawl batch is committed atomically: audit, documents, sections,
    /// and run completion become visible together. Rejections only add audit data.
    pub async fn finish_crawl(&self, batch: ExtractionBatch) -> Result<()> {
        self.finish_prepared_crawl(prepare_crawl(batch, ChunkPolicy::default())?)
            .await
    }

    /// Persist a preparation performed outside the database boundary. A custom
    /// chunk policy can be supplied to ingestion::prepare_crawl.
    pub async fn finish_prepared_crawl(&self, prepared: PreparedCrawl) -> Result<()> {
        let PreparedCrawl { batch, indexes } = prepared;
        ensure!(
            batch.finished_at >= batch.started_at,
            "crawl finished before it started"
        );
        let mut pages = Vec::with_capacity(batch.outcomes.len());
        let mut documents: Vec<Value> = Vec::new();
        let mut identities: HashMap<String, usize> = HashMap::new();
        let mut extracted_count = 0usize;
        for (outcome, index) in batch.outcomes.into_iter().zip(indexes) {
            let page = match &outcome {
                ExtractionOutcome::Extracted(doc) => &doc.page,
                ExtractionOutcome::Rejected { page, .. } => page,
            };
            ensure!(
                page.source_id == batch.source_id && page.crawl_id == batch.crawl_id,
                "page provenance does not match crawl"
            );
            match outcome {
                ExtractionOutcome::Extracted(doc) => {
                    let index = index.context("successful document missing its prepared index")?;
                    validate_sections(&doc)?;
                    extracted_count += 1;
                    pages.push(json!({"page": doc.page, "extraction": "extracted"}));
                    let mut data = serde_json::to_value(&doc)?;
                    let sections = data
                        .as_object_mut()
                        .context("document must be an object")?
                        .remove("sections")
                        .context("document sections missing")?;
                    data["indexing"] = serde_json::to_value(&index.metadata)?;
                    let document = json!({"url": doc.canonical_url, "data": data, "sections": sections, "chunks": index.chunks});
                    if let Some(&index) = identities.get(doc.canonical_url.as_str()) {
                        // Identical redirect aliases share one current document,
                        // while both original fetches remain in the audit.
                        ensure!(
                            documents[index]["data"]["indexing"]["hashes"]["normalized_sha256"]
                                == document["data"]["indexing"]["hashes"]["normalized_sha256"]
                                && documents[index]["data"]["extraction_version"]
                                    == document["data"]["extraction_version"],
                            "conflicting representations of one canonical URL in a crawl"
                        );
                        if doc.page.requested_url == doc.canonical_url.as_str() {
                            documents[index] = document;
                        }
                    } else {
                        identities.insert(doc.canonical_url.to_string(), documents.len());
                        documents.push(document);
                    }
                }
                ExtractionOutcome::Rejected { page, reason } => {
                    pages.push(json!({"page": page, "extraction": "rejected", "reason": reason}))
                }
            }
        }
        let chunk_count: usize = documents
            .iter()
            .filter_map(|document| document["chunks"].as_array())
            .map(Vec::len)
            .sum();
        let oversized_chunk_count: u64 = documents
            .iter()
            .filter_map(|document| document["data"]["indexing"]["oversized_chunk_count"].as_u64())
            .sum();
        let summary = json!({"started_at": batch.started_at, "finished_at": batch.finished_at, "page_count": pages.len(), "document_count": documents.len(), "chunk_count": chunk_count, "oversized_chunk_count": oversized_chunk_count, "alias_count": extracted_count - documents.len(), "rejected_count": pages.len() - extracted_count, "blocked": batch.blocked, "dropped_pages": batch.dropped_pages, "audit_overflow": batch.audit_overflow, "delivery_complete": batch.dropped_pages == 0 && !batch.audit_overflow});
        self.db
            .query(include_str!("finish_crawl.surql"))
            .bind(("source_id", batch.source_id))
            .bind(("crawl_id", batch.crawl_id))
            .bind(("pages", pages))
            .bind(("documents", documents))
            .bind(("summary", summary))
            .bind(("finished", serde_json::to_value(batch.finished_at)?))
            .await?
            .check()?;
        Ok(())
    }

    pub async fn get_chunks(&self, source_id: &str, url: &Url) -> Result<Vec<Chunk>> {
        let mut url = url.clone();
        url.set_fragment(None);
        let mut result = self.db.query("SELECT VALUE data FROM chunk WHERE document = type::record('document', [$source_id, $url]) ORDER BY sequence;")
            .bind(("source_id", source_id.to_owned())).bind(("url", url.to_string())).await?.check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .map(|value| serde_json::from_value(value).map_err(Into::into))
            .collect()
    }

    /// None also covers pre-indexing documents from an earlier app release.
    pub async fn get_indexing(&self, source_id: &str, url: &Url) -> Result<Option<IndexMetadata>> {
        let mut url = url.clone();
        url.set_fragment(None);
        let mut result = self
            .db
            .query("SELECT VALUE data.indexing FROM type::record('document', [$source_id, $url]);")
            .bind(("source_id", source_id.to_owned()))
            .bind(("url", url.to_string()))
            .await?
            .check()?;
        let values: Vec<Value> = result.take(0)?;
        values
            .into_iter()
            .find(|value| !value.is_null())
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn get_document(
        &self,
        source_id: &str,
        url: &Url,
    ) -> Result<Option<ExtractedDocument>> {
        let mut url = url.clone();
        url.set_fragment(None);
        // Both reads share a transaction so concurrent replacement cannot mix versions.
        let mut result = self.db.query("BEGIN TRANSACTION; SELECT VALUE data FROM type::record('document', [$source_id, $url]); SELECT VALUE data FROM section WHERE document = type::record('document', [$source_id, $url]) ORDER BY sequence; COMMIT TRANSACTION;")
            .bind(("source_id", source_id.to_owned())).bind(("url", url.to_string())).await?.check()?;
        let docs: Vec<Value> = result.take(1)?;
        let sections: Vec<Value> = result.take(2)?;
        let Some(mut doc) = docs.into_iter().next() else {
            return Ok(None);
        };
        doc["sections"] = Value::Array(sections);
        Ok(Some(serde_json::from_value(doc)?))
    }

    pub async fn page_outcomes(&self, source_id: &str, crawl_id: &str) -> Result<Vec<Value>> {
        let mut result = self.db.query("SELECT VALUE data FROM page_outcome WHERE crawl = type::record('crawl_run', [$source_id, $crawl_id]) ORDER BY id;")
            .bind(("source_id", source_id.to_owned())).bind(("crawl_id", crawl_id.to_owned())).await?.check()?;
        Ok(result.take(0)?)
    }
}

fn validate_sections(doc: &ExtractedDocument) -> Result<()> {
    let mut seen = HashSet::new();
    for (sequence, section) in doc.sections.iter().enumerate() {
        ensure!(
            section.id == sequence,
            "section IDs must match document order"
        );
        ensure!(
            section.parent_id.is_none_or(|id| seen.contains(&id)),
            "section parent must precede its child"
        );
        seen.insert(section.id);
    }
    ensure!(!seen.is_empty(), "document has no sections");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crawler::{PageOutcome, PageState},
        extraction::extract,
        retrieval::{SearchQuery, search},
    };

    #[tokio::test]
    async fn lexical_schema_upgrade_indexes_existing_chunks_without_recrawling() {
        let directory = tempfile::TempDir::new().unwrap();
        let db = connect((
            format!("surrealkv://{}", directory.path().display()),
            Config::new().capabilities(Capabilities::default().with_all_functions_allowed()),
        ))
        .await
        .unwrap();
        db.use_ns("ariadne").use_db("knowledge").await.unwrap();
        let previous_sql = SCHEMA[0].sql.split("DEFINE ANALYZER").next().unwrap();
        Sync::embedded(&[EmbeddedSchemaFile {
            path: SCHEMA[0].path,
            sql: previous_sql,
        }])
        .prune(false)
        .run(&db)
        .await
        .unwrap();
        let store = KnowledgeStore {
            db,
            path: directory.path().to_owned(),
        };
        let url = Url::parse("https://example.test/docs/proxy").unwrap();
        store
            .register_source(Source::new("docs", "Docs", url.clone()).unwrap())
            .await
            .unwrap();
        let request = CrawlRequest::new(
            "docs",
            "existing",
            url.clone(),
            CrawlScope::new(url.clone(), "/docs").unwrap(),
        );
        store.begin_crawl(&request).await.unwrap();
        let now = SystemTime::now();
        let outcome = extract(PageOutcome { source_id: "docs".into(), crawl_id: "existing".into(), requested_url: url.to_string(), final_url: url.to_string(), fetched_at: now, status: 200, headers: vec![], raw_body: b"<title>Proxy</title><main><h1 id='custom'>Custom configuration</h1><p>Proxy::custom() supports custom SOCKS proxies.</p></main>".to_vec(), content_truncated: false, state: PageState::Fetched });
        store
            .finish_crawl(ExtractionBatch {
                source_id: "docs".into(),
                crawl_id: "existing".into(),
                started_at: now,
                finished_at: now,
                outcomes: vec![outcome],
                blocked: vec![],
                dropped_pages: 0,
                audit_overflow: false,
            })
            .await
            .unwrap();
        let chunks = store.get_chunks("docs", &url).await.unwrap();
        drop(store);
        let mut reopened = None;
        for _ in 0..100 {
            match KnowledgeStore::open(directory.path()).await {
                Ok(store) => {
                    reopened = Some(store);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
        let store = reopened.expect("upgraded database opens");
        let hits = search(&store, SearchQuery::new("Proxy::custom"))
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].chunk_id, chunks[0].id);
        assert_eq!(hits[0].crawl_id, "existing");
        assert_eq!(store.get_chunks("docs", &url).await.unwrap(), chunks);
    }

    #[tokio::test]
    async fn surrealkit_upgrade_retains_pre_chunking_documents() {
        let directory = tempfile::TempDir::new().unwrap();
        let endpoint = format!("surrealkv://{}", directory.path().display());
        let db = connect((
            endpoint,
            Config::new().capabilities(Capabilities::default().with_all_functions_allowed()),
        ))
        .await
        .unwrap();
        db.use_ns("ariadne").use_db("knowledge").await.unwrap();
        let previous_sql = SCHEMA[0].sql.split("DEFINE TABLE chunk").next().unwrap();
        Sync::embedded(&[EmbeddedSchemaFile {
            path: SCHEMA[0].path,
            sql: previous_sql,
        }])
        .prune(false)
        .run(&db)
        .await
        .unwrap();
        let url = Url::parse("https://example.test/docs/").unwrap();
        let outcome = extract(PageOutcome {
            source_id: "docs".into(),
            crawl_id: "old".into(),
            requested_url: url.to_string(),
            final_url: url.to_string(),
            fetched_at: SystemTime::now(),
            status: 200,
            headers: vec![],
            raw_body: b"<main><h1>Client</h1><p>Existing documentation.</p></main>".to_vec(),
            content_truncated: false,
            state: PageState::Fetched,
        });
        let ExtractionOutcome::Extracted(document) = outcome else {
            panic!("expected document")
        };
        let mut data = serde_json::to_value(&document).unwrap();
        let sections = data.as_object_mut().unwrap().remove("sections").unwrap();
        db.query("BEGIN TRANSACTION; CREATE source:docs SET data = $source; CREATE type::record('document', ['docs', $url]) SET source = source:docs, crawl = type::record('crawl_run', ['docs', 'old']), canonical_url = $url, data = $data; FOR $section IN $sections { CREATE type::record('section', ['docs', $url, $section.id]) SET document = type::record('document', ['docs', $url]), sequence = $section.id, data = $section; }; COMMIT TRANSACTION;")
            .bind(("source", serde_json::to_value(Source::new("docs", "Docs", url.clone()).unwrap()).unwrap())).bind(("url", url.to_string())).bind(("data", data)).bind(("sections", sections)).await.unwrap().check().unwrap();
        drop(db);
        let mut store = None;
        for _ in 0..100 {
            match KnowledgeStore::open(directory.path()).await {
                Ok(opened) => {
                    store = Some(opened);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
        let store = store.expect("upgraded database opens");
        assert_eq!(
            store
                .get_document("docs", &url)
                .await
                .unwrap()
                .unwrap()
                .markdown(),
            document.markdown()
        );
        assert!(store.get_indexing("docs", &url).await.unwrap().is_none());
        assert!(store.get_chunks("docs", &url).await.unwrap().is_empty());
        let request = CrawlRequest::new(
            "docs",
            "new",
            url.clone(),
            CrawlScope::new(url.clone(), "/docs").unwrap(),
        );
        store.begin_crawl(&request).await.unwrap();
        let mut document = document;
        document.page.crawl_id = "new".into();
        let now = SystemTime::now();
        store
            .finish_crawl(ExtractionBatch {
                source_id: "docs".into(),
                crawl_id: "new".into(),
                started_at: now,
                finished_at: now,
                outcomes: vec![ExtractionOutcome::Extracted(document)],
                blocked: vec![],
                dropped_pages: 0,
                audit_overflow: false,
            })
            .await
            .unwrap();
        assert!(!store.get_chunks("docs", &url).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn database_error_rolls_back_audit_and_document_replacement() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = KnowledgeStore::open(dir.path()).await.unwrap();
        let url = Url::parse("https://example.test/docs/").unwrap();
        store
            .register_source(Source::new("docs", "Docs", url.clone()).unwrap())
            .await
            .unwrap();
        let request = |run| {
            CrawlRequest::new(
                "docs",
                run,
                url.clone(),
                CrawlScope::new(url.clone(), "/docs").unwrap(),
            )
        };
        let outcome = |run: &str, title: &str| {
            extract(PageOutcome {
                source_id: "docs".into(),
                crawl_id: run.into(),
                requested_url: url.to_string(),
                final_url: url.to_string(),
                fetched_at: SystemTime::now(),
                status: 200,
                headers: vec![],
                raw_body: format!(
                    "<title>{title}</title><main><h1>{title}</h1><p>content</p></main>"
                )
                .into_bytes(),
                content_truncated: false,
                state: PageState::Fetched,
            })
        };
        let batch = |run: &str, title| {
            let now = SystemTime::now();
            ExtractionBatch {
                source_id: "docs".into(),
                crawl_id: run.into(),
                started_at: now,
                finished_at: now,
                outcomes: vec![outcome(run, title)],
                blocked: vec![],
                dropped_pages: 0,
                audit_overflow: false,
            }
        };
        store.begin_crawl(&request("good")).await.unwrap();
        store.finish_crawl(batch("good", "Original")).await.unwrap();
        let original =
            serde_json::to_value(store.get_document("docs", &url).await.unwrap()).unwrap();
        let original_chunks = store.get_chunks("docs", &url).await.unwrap();
        let original_indexing = store.get_indexing("docs", &url).await.unwrap();
        let original_search = search(&store, SearchQuery::new("Original")).await.unwrap();
        assert!(!original_search.is_empty());
        // Force a storage error after audits have been inserted and old sections
        // deleted, exercising a real database rollback rather than prevalidation.
        store
            .db
            .query("DEFINE FIELD data.title ON document TYPE string ASSERT $value != 'Rejected';")
            .await
            .unwrap()
            .check()
            .unwrap();
        store.begin_crawl(&request("bad")).await.unwrap();
        assert!(store.finish_crawl(batch("bad", "Rejected")).await.is_err());
        assert_eq!(
            serde_json::to_value(store.get_document("docs", &url).await.unwrap()).unwrap(),
            original
        );
        assert!(store.page_outcomes("docs", "bad").await.unwrap().is_empty());
        assert_eq!(
            store.get_chunks("docs", &url).await.unwrap(),
            original_chunks
        );
        assert_eq!(
            store.get_indexing("docs", &url).await.unwrap(),
            original_indexing
        );
        assert_eq!(
            search(&store, SearchQuery::new("Original")).await.unwrap(),
            original_search
        );
        assert!(
            search(&store, SearchQuery::new("Rejected"))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .get_crawl("docs", "bad")
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
    }
}

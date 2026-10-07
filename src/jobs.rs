//! Bounded, supervised ingestion jobs. Persistence is owned by storage.
use crate::{
    chunking::ChunkPolicy,
    crawler::{CrawlRequest, CrawlScope, network::validate_public_url},
    embeddings::{OllamaConfig, OllamaProvider},
    ingestion::{ingest, ingest_lexical, recrawl, recrawl_lexical},
    storage::{CrawlRun, KnowledgeStore},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::{Semaphore, watch};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobOperation {
    Crawl,
    Recrawl,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Partial,
    Failed,
    Cancelled,
    Interrupted,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: String,
    pub source_id: String,
    pub crawl_id: String,
    pub operation: JobOperation,
    pub status: JobStatus,
    pub created_at: SystemTime,
    pub finished_at: Option<SystemTime>,
    pub elapsed_ms: Option<u64>,
    pub configuration: serde_json::Value,
    pub error: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub job: JobRecord,
    pub run: Option<CrawlRun>,
}

#[derive(Debug, Clone, Default)]
pub struct CrawlAccess {
    pub allowed_sources: HashSet<String>,
    pub allow_private_network: bool,
    pub browser_fallback: bool,
}
impl CrawlAccess {
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var("ARIADNE_MCP_CRAWL_SOURCES").unwrap_or_default();
        let allowed_sources = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
        let allow_private_network = match std::env::var("ARIADNE_MCP_ALLOW_PRIVATE_NETWORK")
            .ok()
            .as_deref()
        {
            None | Some("0") => false,
            Some("1") => true,
            _ => anyhow::bail!("ARIADNE_MCP_ALLOW_PRIVATE_NETWORK must be 0 or 1"),
        };
        let browser_fallback = match std::env::var("ARIADNE_BROWSER_FALLBACK").ok().as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            _ => anyhow::bail!("ARIADNE_BROWSER_FALLBACK must be 0 or 1"),
        };
        Ok(Self {
            browser_fallback,
            allowed_sources,
            allow_private_network,
        })
    }
}
struct Control {
    cancel: watch::Sender<bool>,
    done: watch::Receiver<bool>,
}
pub struct JobManager {
    store: Arc<KnowledgeStore>,
    config: OllamaConfig,
    access: CrawlAccess,
    slots: Arc<Semaphore>,
    active: Mutex<HashMap<String, Control>>,
    stopping: AtomicBool,
}
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
impl JobManager {
    pub fn new(store: Arc<KnowledgeStore>, config: OllamaConfig, access: CrawlAccess) -> Arc<Self> {
        Arc::new(Self {
            store,
            config,
            access,
            slots: Arc::new(Semaphore::new(2)),
            active: Mutex::new(HashMap::new()),
            stopping: AtomicBool::new(false),
        })
    }
    pub async fn start(
        self: &Arc<Self>,
        source_id: &str,
        operation: JobOperation,
        max_pages: u32,
        lexical_only: bool,
        discovery: bool,
    ) -> Result<JobRecord> {
        ensure!(
            !self.stopping.load(Ordering::SeqCst),
            "job service is stopping"
        );
        ensure!(
            self.access.allowed_sources.contains(source_id),
            "source is not authorized for MCP crawling; configure ARIADNE_MCP_CRAWL_SOURCES"
        );
        ensure!(
            (1..=200).contains(&max_pages),
            "agent crawl budget must be 1..200 pages"
        );
        let source = self
            .store
            .get_source(source_id)
            .await?
            .context("source not found; register it with the CLI")?;
        if !self.access.allow_private_network {
            validate_public_url(&source.root_url)?;
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .context("two crawl jobs are already active; retry later")?;
        let now = SystemTime::now();
        let id = format!(
            "job-{}",
            crate::chunking::sha256(&serde_json::to_vec(&(
                now,
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ))?)
        );
        let scope = CrawlScope::new(source.root_url.clone(), source.root_url.path())?;
        let mut request = CrawlRequest::new(
            source.id.clone(),
            id.clone(),
            source.root_url.clone(),
            scope,
        );
        request.max_pages = max_pages;
        request.public_network_only = !self.access.allow_private_network;
        request.allow_loopback_redirects = self.access.allow_private_network;
        request.discovery = discovery;
        request.browser_fallback = self.access.browser_fallback;
        request.validate()?;
        let job = JobRecord {
            id: id.clone(),
            source_id: source.id.clone(),
            crawl_id: id.clone(),
            operation,
            status: JobStatus::Queued,
            created_at: now,
            finished_at: None,
            elapsed_ms: None,
            configuration: serde_json::json!({"request":request,"lexical_only":lexical_only,"deadline_seconds":300}),
            error: None,
        };
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (done_tx, done_rx) = watch::channel(false);
        // Serialize admission with shutdown, and make cancellation observable
        // before the job is returned. The map is capped by the two permits.
        {
            let mut active = self.active.lock().unwrap();
            ensure!(
                !self.stopping.load(Ordering::SeqCst),
                "job service is stopping"
            );
            active.insert(
                id.clone(),
                Control {
                    cancel: cancel_tx,
                    done: done_rx,
                },
            );
        }
        // Admission owns its durable write independently of the caller's RPC
        // cancellation, so an accepted row can never be orphaned without a worker.
        let admitter = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(error) = admitter.store.reserve_job(&job).await {
                admitter.active.lock().unwrap().remove(&id);
                return Err(error);
            }
            let manager = Arc::clone(&admitter);
            let config = admitter.config.clone();
            let store = Arc::clone(&admitter.store);
            let task_id = id.clone();
            tokio::spawn(async move {
                let result = async {
                    store.start_job(&task_id).await?;
                    let mut execution = tokio::spawn(async move {
                        let policy = ChunkPolicy::default();
                        match (operation, lexical_only) {
                            (JobOperation::Crawl, true) => {
                                Box::pin(ingest_lexical(&store, source, request, policy)).await
                            }
                            (JobOperation::Recrawl, true) => {
                                Box::pin(recrawl_lexical(&store, source, request, policy)).await
                            }
                            (JobOperation::Crawl, false) => {
                                Box::pin(ingest(&store, source, request, policy, || {
                                    OllamaProvider::connect(config)
                                }))
                                .await
                            }
                            (JobOperation::Recrawl, false) => {
                                Box::pin(recrawl(&store, source, request, policy, || {
                                    OllamaProvider::connect(config)
                                }))
                                .await
                            }
                        }
                    });
                    let result = tokio::select! {
                        biased;
                        value=&mut execution=>Some(value),
                        _=async {if !*cancel_rx.borrow() {let _=cancel_rx.changed().await;}}=>None,
                        _=tokio::time::sleep(Duration::from_secs(300))=>{
                            execution.abort();let _=execution.await;
                            return Ok((JobStatus::Failed,Some("job deadline exceeded".to_owned())));
                        },
                    };
                    match result {
                        None => {
                            execution.abort();
                            let _ = execution.await;
                            Ok((JobStatus::Cancelled, None))
                        }
                        Some(Ok(Ok(run))) => {
                            let partial = run.ingestion.is_some_and(|p| {
                                p.status != crate::ingestion::IngestionStatus::Completed
                            });
                            Ok((
                                if partial {
                                    JobStatus::Partial
                                } else {
                                    JobStatus::Completed
                                },
                                None,
                            ))
                        }
                        Some(error) => {
                            tracing::warn!(job_id=%task_id,error=?error,"ingestion job failed");
                            let run = manager
                                .store
                                .get_crawl(
                                    &manager
                                        .store
                                        .get_job(&task_id)
                                        .await?
                                        .context("job missing")?
                                        .job
                                        .source_id,
                                    &task_id,
                                )
                                .await?;
                            let status = if run.is_some_and(|r| r.status == "completed") {
                                JobStatus::Partial
                            } else {
                                JobStatus::Failed
                            };
                            Ok((
                                status,
                                Some("ingestion failed; inspect server stderr".to_owned()),
                            ))
                        }
                    }
                }
                .await;
                let (status, error) = result.unwrap_or_else(|error: anyhow::Error| {
                    tracing::error!(job_id=%task_id,error=%error,"job supervision failed");
                    (
                        JobStatus::Failed,
                        Some("job supervision failed; inspect server stderr".into()),
                    )
                });
                if let Err(error) = manager.store.finish_job(&task_id, status, error).await {
                    tracing::error!(job_id=%task_id,error=%error,"persist terminal job status failed");
                }
                manager.active.lock().unwrap().remove(&task_id);
                let _ = done_tx.send(true);
                drop(permit);
            });
            Ok(job)
        }).await.context("join job admission")?
    }
    pub async fn status(&self, id: &str) -> Result<JobSnapshot> {
        self.store.get_job(id).await?.context("job not found")
    }
    pub async fn cancel(&self, id: &str) -> Result<JobSnapshot> {
        let mut done = {
            let active = self.active.lock().unwrap();
            active.get(id).map(|control| {
                let _ = control.cancel.send(true);
                control.done.clone()
            })
        };
        if let Some(done) = done.as_mut() {
            let _ = tokio::time::timeout(Duration::from_secs(5), async {
                if !*done.borrow() {
                    let _ = done.changed().await;
                }
            })
            .await;
        }
        self.status(id).await
    }
    pub fn request_shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        for control in self.active.lock().unwrap().values() {
            let _ = control.cancel.send(true);
        }
    }
    pub async fn shutdown(&self) {
        self.request_shutdown();
        let receivers: Vec<_> = self
            .active
            .lock()
            .unwrap()
            .values()
            .map(|c| c.done.clone())
            .collect();
        for mut receiver in receivers {
            let _ = tokio::time::timeout(Duration::from_secs(5), async {
                if !*receiver.borrow() {
                    let _ = receiver.changed().await;
                }
            })
            .await;
        }
    }
}

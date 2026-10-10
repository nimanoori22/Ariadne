//! One datastore owner, shared by all HTTP clients.
use super::KnowledgeMcp;
use crate::{embeddings::OllamaConfig, jobs::CrawlAccess, storage::KnowledgeStore};
use anyhow::{Context, Result, ensure};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::{SessionManager, local::LocalSessionManager},
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Serve `/mcp` on a loopback listener until shutdown is requested.
/// All sessions share admission limits, embeddings and supervised crawl jobs.
pub async fn serve_http(
    store: KnowledgeStore,
    config: OllamaConfig,
    access: CrawlAccess,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<()> {
    let address = listener.local_addr()?;
    ensure!(address.ip().is_loopback(), "MCP HTTP must bind to loopback");
    let handler = Arc::new(KnowledgeMcp::with_access(store, config, access));
    let jobs = handler.jobs.clone();
    let transport_config = StreamableHttpServerConfig::default()
        .enforce_origin_validation()
        .with_cancellation_token(shutdown.child_token());
    let transport_shutdown = transport_config.cancellation_token.clone();
    let sessions = Arc::new(LocalSessionManager::default());
    // Cloning the Arc keeps session teardown from shutting down shared jobs.
    let service: StreamableHttpService<Arc<KnowledgeMcp>, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(handler.clone()),
            sessions.clone(),
            transport_config,
        );
    let router = axum::Router::new().nest_service("/mcp", service);
    tracing::info!(%address, "shared MCP HTTP server listening at /mcp");
    let stopping_jobs = jobs.clone();
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            stopping_jobs.request_shutdown();
        })
        .await;
    transport_shutdown.cancel();
    // The SDK's legacy session workers outlive the HTTP listener. Close them
    // explicitly so they drop the shared datastore rather than retain its lock.
    let session_ids: Vec<_> = sessions.sessions.read().await.keys().cloned().collect();
    for id in session_ids {
        sessions
            .close_session(&id)
            .await
            .context("close MCP session")?;
    }
    jobs.shutdown().await;
    result.context("serve MCP HTTP")
}

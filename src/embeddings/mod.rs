//! Derived, model-scoped vectors. Chunk content remains the source of truth.
mod ollama;
pub use ollama::{OllamaConfig, OllamaProvider};

use crate::{chunking::sha256, storage::KnowledgeStore};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingSpace {
    pub provider: String,
    pub model: String,
    /// Immutable weights identity, rather than a mutable model tag.
    pub revision: String,
    pub dimensions: usize,
    /// Includes document/query formatting and normalization policy.
    pub input_version: String,
}
impl EmbeddingSpace {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=4096).contains(&self.dimensions),
            "embedding dimensions must be 1..4096"
        );
        for field in [
            &self.provider,
            &self.model,
            &self.revision,
            &self.input_version,
        ] {
            ensure!(
                !field.trim().is_empty() && field.len() <= 1024,
                "invalid embedding identity"
            );
        }
        Ok(())
    }
    pub fn id(&self) -> String {
        // Fixed struct field order makes the serialized identity deterministic.
        sha256(&serde_json::to_vec(self).expect("string identity serializes"))
    }
    pub(crate) fn table(&self) -> String {
        format!("embedding_{}", self.id())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingPurpose {
    Document,
    Query,
}

#[derive(Debug, Clone, Copy)]
pub struct ProviderLimits {
    pub batch_size: usize,
    /// Resource cap, not a claim about a model's tokenizer.
    pub max_input_bytes: usize,
    pub max_batch_bytes: usize,
}
impl ProviderLimits {
    pub fn validate(self) -> Result<()> {
        ensure!(
            (1..=32).contains(&self.batch_size),
            "invalid embedding batch size"
        );
        ensure!(
            (1..=128_000).contains(&self.max_input_bytes),
            "invalid embedding input cap"
        );
        ensure!(
            (self.max_input_bytes..=1_000_000).contains(&self.max_batch_bytes),
            "invalid embedding batch cap"
        );
        Ok(())
    }
}

pub trait EmbeddingProvider: Sync {
    fn space(&self) -> &EmbeddingSpace;
    fn limits(&self) -> ProviderLimits;
    fn embed(
        &self,
        inputs: &[String],
        purpose: EmbeddingPurpose,
    ) -> impl Future<Output = Result<Vec<Vec<f64>>>> + Send;
}

pub(crate) fn normalize(vector: &mut [f64], dimensions: usize) -> Result<()> {
    ensure!(vector.len() == dimensions, "embedding dimension mismatch");
    ensure!(vector.iter().all(|n| n.is_finite()), "non-finite embedding");
    let magnitude = vector.iter().fold(0.0_f64, |norm, n| norm.hypot(*n));
    ensure!(
        magnitude.is_finite() && magnitude > 0.0,
        "invalid or zero embedding"
    );
    for value in vector {
        *value /= magnitude;
    }
    Ok(())
}

pub async fn embed_checked<P: EmbeddingProvider>(
    provider: &P,
    inputs: &[String],
    purpose: EmbeddingPurpose,
) -> Result<Vec<Vec<f64>>> {
    provider.space().validate()?;
    let limits = provider.limits();
    limits.validate()?;
    ensure!(
        !inputs.is_empty() && inputs.len() <= limits.batch_size,
        "embedding batch exceeds provider limit"
    );
    ensure!(
        inputs
            .iter()
            .all(|input| !input.trim().is_empty() && input.len() <= limits.max_input_bytes),
        "embedding input exceeds provider limit or is empty"
    );
    ensure!(
        inputs.iter().map(String::len).sum::<usize>() <= limits.max_batch_bytes,
        "embedding batch byte limit exceeded"
    );
    let mut vectors =
        tokio::time::timeout(Duration::from_secs(120), provider.embed(inputs, purpose)).await??;
    ensure!(
        vectors.len() == inputs.len(),
        "embedding response count mismatch"
    );
    for vector in &mut vectors {
        normalize(vector, provider.space().dimensions)?;
    }
    Ok(vectors)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingStatus {
    Pending,
    Ready,
    Failed,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingState {
    pub chunk_id: String,
    pub content_sha256: String,
    pub status: EmbeddingStatus,
    pub attempts: usize,
    pub created_at: Option<SystemTime>,
    pub updated_at: SystemTime,
    pub error: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EmbeddingReport {
    pub space_id: String,
    pub scanned: usize,
    pub reused: usize,
    pub generated: usize,
    pub failed: usize,
    /// A chunk was replaced or another producer completed it during generation.
    pub superseded: usize,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EmbeddingCoverage {
    pub current_chunks: usize,
    pub ready: usize,
    pub pending: usize,
    pub failed: usize,
    pub missing: usize,
    /// Historical derivatives, excluded from search and current chunk counts.
    pub stale: usize,
}

/// Sequential bounded batches give local inference concurrency of one. Every
/// batch is checkpointed; a cancelled future leaves pending work resumable.
/// Failed batches are isolated into individual calls so one oversized/model-
/// rejected input cannot prevent the other chunks from becoming searchable.
pub async fn index_source<P: EmbeddingProvider>(
    store: &KnowledgeStore,
    provider: &P,
    source_id: &str,
) -> Result<EmbeddingReport> {
    ensure!(
        store.get_source(source_id).await?.is_some(),
        "unknown source"
    );
    let space = provider.space();
    space.validate()?;
    let limits = provider.limits();
    limits.validate()?;
    store.ensure_embedding_space(space).await?;
    let mut report = EmbeddingReport {
        space_id: space.id(),
        ..Default::default()
    };
    let mut cursor = String::new();
    loop {
        let chunks = store
            .embedding_chunks(source_id, &cursor, limits.batch_size)
            .await?;
        if chunks.is_empty() {
            break;
        }
        cursor = chunks.last().expect("nonempty").id.clone();
        report.scanned += chunks.len();
        let mut pending = Vec::new();
        for chunk in chunks {
            if store.prepare_embedding(space, &chunk).await? {
                pending.push(chunk);
            } else {
                report.reused += 1;
            }
        }
        if pending.is_empty() {
            continue;
        }
        let inputs: Vec<String> = pending.iter().map(|c| c.markdown.clone()).collect();
        match embed_checked(provider, &inputs, EmbeddingPurpose::Document).await {
            Ok(vectors) => {
                for (chunk, vector) in pending.iter().zip(vectors) {
                    if store
                        .complete_embedding(space, chunk, Some(vector), None)
                        .await?
                    {
                        report.generated += 1;
                    } else {
                        report.superseded += 1;
                    }
                }
            }
            Err(batch_error) => {
                // A failed single input has already had the provider's retry
                // policy applied; do not repeat it here.
                for (chunk, input) in pending.iter().zip(inputs) {
                    let result = if pending.len() == 1 {
                        Err(anyhow::anyhow!("{batch_error:#}"))
                    } else {
                        embed_checked(provider, &[input], EmbeddingPurpose::Document)
                            .await
                            .map(|mut v| v.remove(0))
                    };
                    let (vector, error) = match result {
                        Ok(vector) => (Some(vector), None),
                        Err(error) => (
                            None,
                            Some(format!("{error:#}").chars().take(1000).collect::<String>()),
                        ),
                    };
                    let success = vector.is_some();
                    if !store
                        .complete_embedding(space, chunk, vector, error)
                        .await?
                    {
                        report.superseded += 1;
                    } else if success {
                        report.generated += 1;
                    } else {
                        report.failed += 1;
                    }
                }
            }
        }
    }
    tracing::info!(
        source_id,
        space_id = report.space_id,
        generated = report.generated,
        failed = report.failed,
        "embedding run finished"
    );
    Ok(report)
}

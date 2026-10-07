use super::{EmbeddingProvider, EmbeddingPurpose, EmbeddingSpace, ProviderLimits, normalize};
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub struct OllamaConfig {
    pub endpoint: Url,
    pub model: String,
}
impl OllamaConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            endpoint: Url::parse(
                &std::env::var("ARIADNE_OLLAMA_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:11434/".into()),
            )?,
            model: std::env::var("ARIADNE_EMBED_MODEL")
                .unwrap_or_else(|_| "embeddinggemma:latest".into()),
        })
    }
}

pub struct OllamaProvider {
    client: Client,
    config: OllamaConfig,
    space: EmbeddingSpace,
}
impl OllamaProvider {
    /// Resolves a mutable tag to its installed digest and probes dimensions.
    /// Model installation and service startup remain explicit user operations.
    pub async fn connect(mut config: OllamaConfig) -> Result<Self> {
        ensure!(
            ["http", "https"].contains(&config.endpoint.scheme())
                && config.endpoint.host_str().is_some(),
            "invalid Ollama URL"
        );
        ensure!(
            config.endpoint.username().is_empty()
                && config.endpoint.password().is_none()
                && config.endpoint.query().is_none()
                && config.endpoint.fragment().is_none(),
            "Ollama URL must not contain credentials, query or fragment"
        );
        ensure!(
            !config.model.trim().is_empty() && config.model.len() <= 256,
            "invalid Ollama model"
        );
        if !config.endpoint.path().ends_with('/') {
            let path = format!("{}/", config.endpoint.path());
            config.endpoint.set_path(&path);
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let (model, revision) = resolve(&client, &config)
            .await
            .context("connect to Ollama; start Ollama and pull the embedding model first")?;
        config.model = model;
        let mut provider = Self {
            client,
            config,
            space: EmbeddingSpace {
                provider: "ollama".into(),
                model: String::new(),
                revision,
                dimensions: 1,
                input_version: "markdown-raw-v1-l2".into(),
            },
        };
        provider.space.model = provider.config.model.clone();
        let probe = ["embedding dimension probe".into()];
        // A first load can materialize/convert a runner-specific manifest.
        // Probe output is never persisted. If identity changes during warmup,
        // discard it and require a second probe under the stable new identity.
        let mut vectors = provider.request_raw(&probe).await?;
        let (_, revision) = resolve(&provider.client, &provider.config).await?;
        if revision != provider.space.revision {
            provider.space.revision = revision;
            vectors = provider.request(&probe).await?;
        }
        ensure!(vectors.len() == 1, "embedding probe count mismatch");
        provider.space.dimensions = vectors[0].len();
        provider.space.validate()?;
        normalize(&mut vectors[0], provider.space.dimensions)?;
        Ok(provider)
    }

    async fn check_revision(&self) -> Result<()> {
        let (_, digest) = resolve(&self.client, &self.config).await?;
        ensure!(
            digest == self.space.revision,
            "Ollama model changed during this session; reconnect to create a new vector space"
        );
        Ok(())
    }

    async fn request(&self, inputs: &[String]) -> Result<Vec<Vec<f64>>> {
        self.check_revision().await?;
        let vectors = self.request_raw(inputs).await?;
        self.check_revision().await?;
        Ok(vectors)
    }

    async fn request_raw(&self, inputs: &[String]) -> Result<Vec<Vec<f64>>> {
        let url = self.config.endpoint.join("api/embed")?;
        for attempt in 0..3 {
            let response = self
                .client
                .post(url.clone())
                .json(&json!({"model": self.config.model, "input": inputs, "truncate": false}))
                .send()
                .await
                .context("Ollama embedding request")?;
            let status = response.status();
            if (status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()) && attempt < 2
            {
                let delay = match response.headers().get("retry-after") {
                    Some(value) => match value.to_str()?.parse::<u64>() {
                        Ok(seconds) if seconds <= 5 => Duration::from_secs(seconds),
                        _ => bail!(
                            "Ollama requested an unsupported or excessive Retry-After; retry this run later"
                        ),
                    },
                    None => Duration::from_millis(100 * (1 << attempt)),
                };
                tokio::time::sleep(delay).await;
                continue;
            }
            // Do not persist/log a provider's response body: it may echo source
            // text or credentials. Status and local diagnostics are sufficient.
            ensure!(status.is_success(), "Ollama embedding HTTP {status}");
            let response: EmbedResponse = bounded_json(response, 4_000_000).await?;
            ensure!(
                response.model == self.config.model,
                "Ollama returned a different model"
            );
            return Ok(response.embeddings);
        }
        unreachable!("last attempt returns or errors")
    }
}
impl EmbeddingProvider for OllamaProvider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }
    fn limits(&self) -> ProviderLimits {
        ProviderLimits {
            batch_size: 16,
            max_input_bytes: 8192,
            max_batch_bytes: 65536,
        }
    }
    async fn embed(&self, inputs: &[String], _purpose: EmbeddingPurpose) -> Result<Vec<Vec<f64>>> {
        self.request(inputs).await
    }
}

#[derive(Deserialize)]
struct EmbedResponse {
    model: String,
    embeddings: Vec<Vec<f64>>,
}
#[derive(Deserialize)]
struct Tags {
    models: Vec<Tag>,
}
#[derive(Deserialize)]
struct Tag {
    name: String,
    digest: String,
}
async fn resolve(client: &Client, config: &OllamaConfig) -> Result<(String, String)> {
    let response = client
        .get(config.endpoint.join("api/tags")?)
        .send()
        .await?
        .error_for_status()?;
    let tags: Tags = bounded_json(response, 1_000_000).await?;
    let target = if config
        .model
        .rsplit('/')
        .next()
        .is_some_and(|name| name.contains(':'))
    {
        config.model.clone()
    } else {
        format!("{}:latest", config.model)
    };
    let matches: Vec<Tag> = tags
        .models
        .into_iter()
        .filter(|tag| tag.name == target)
        .collect();
    ensure!(
        !matches.is_empty(),
        "requested embedding model is not installed"
    );
    for tag in &matches {
        validate_digest(&tag.digest)?;
    }
    let mut digest = matches[0].digest.clone();
    if matches.iter().any(|tag| tag.digest != digest) {
        // Ollama 0.40 can return multiple runner variants under one tag. Never
        // depend on their listing order; /api/show identifies the selected one.
        let response = client
            .post(config.endpoint.join("api/show")?)
            .json(&json!({"model": target}))
            .send()
            .await?
            .error_for_status()?;
        let details: Show = bounded_json(response, 1_000_000).await?;
        let selected: Vec<Manifest> = details
            .manifests
            .into_iter()
            .filter(|m| m.selected)
            .collect();
        ensure!(
            selected.len() == 1,
            "ambiguous Ollama model variants: no unique selected manifest"
        );
        digest = selected[0]
            .digest
            .strip_prefix("sha256:")
            .unwrap_or(&selected[0].digest)
            .to_owned();
        validate_digest(&digest)?;
        ensure!(
            matches.iter().any(|tag| tag.digest == digest),
            "selected Ollama manifest is not installed"
        );
    }
    Ok((target, digest))
}

fn validate_digest(digest: &str) -> Result<()> {
    ensure!(
        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid Ollama model digest"
    );
    Ok(())
}
#[derive(Deserialize)]
struct Show {
    #[serde(default)]
    manifests: Vec<Manifest>,
}
#[derive(Deserialize)]
struct Manifest {
    digest: String,
    #[serde(default)]
    selected: bool,
}

async fn bounded_json<T: for<'de> Deserialize<'de>>(
    mut response: reqwest::Response,
    cap: usize,
) -> Result<T> {
    ensure!(
        response.content_length().is_none_or(|n| n <= cap as u64),
        "Ollama response exceeds limit"
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= cap,
            "Ollama response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    // Decode without exposing the payload in error messages.
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("invalid Ollama JSON"))?;
    serde_json::from_value(value).map_err(|_| anyhow::anyhow!("invalid Ollama response shape"))
}

//! Multiple clients share one datastore owner and survive other clients disconnecting.
use ariadne::{
    embeddings::OllamaConfig,
    jobs::CrawlAccess,
    storage::{KnowledgeStore, Source},
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

async fn rpc(
    client: &reqwest::Client,
    url: &str,
    session: Option<&str>,
    body: Value,
) -> (Option<String>, Value) {
    let mut request = client
        .post(url)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-03-26")
        .json(&body);
    if let Some(session) = session {
        request = request.header("Mcp-Session-Id", session);
    }
    let response = request.send().await.unwrap().error_for_status().unwrap();
    let session = response
        .headers()
        .get("Mcp-Session-Id")
        .map(|v| v.to_str().unwrap().to_owned());
    let body = response.text().await.unwrap();
    let json_text = body
        .lines()
        .find_map(|line| {
            line.strip_prefix("data: ")
                .filter(|data| !data.trim().is_empty())
        })
        .unwrap_or(&body);
    (session, serde_json::from_str(json_text).unwrap())
}

async fn initialize(client: &reqwest::Client, url: &str) -> String {
    let (session, response) = rpc(client, url, None, json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-03-26", "capabilities":{}, "clientInfo":{"name":"fixture","version":"1"}
    }})).await;
    assert_eq!(response["result"]["serverInfo"]["name"], "ariadne");
    let session = session.unwrap();
    client
        .post(url)
        .header("Accept", "application/json, text/event-stream")
        .header("Mcp-Session-Id", &session)
        .header("MCP-Protocol-Version", "2025-03-26")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    session
}

#[tokio::test]
async fn two_sessions_share_storage_disconnect_and_release_on_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let fixture_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fixture_url = url::Url::parse(&format!(
        "http://{}/docs/",
        fixture_listener.local_addr().unwrap()
    ))
    .unwrap();
    let fixture = tokio::spawn(async move {
        loop {
            let (mut socket, _) = fixture_listener.accept().await.unwrap();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut request = [0; 4096];
                let count = socket.read(&mut request).await.unwrap();
                let robots = String::from_utf8_lossy(&request[..count]).contains("/robots.txt");
                let (kind, body) = if robots {
                    ("text/plain", "User-agent: *\nAllow: /\n")
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    (
                        "text/html",
                        "<html><title>Fixture docs</title><main><h1>Proxy</h1><p>Configure an HTTP proxy for outgoing connections.</p></main></html>",
                    )
                };
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            });
        }
    });
    let store = KnowledgeStore::open(directory.path()).await.unwrap();
    store
        .register_source(Source::new("fixture", "Fixture docs", fixture_url).unwrap())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(ariadne::mcp::serve_http(
        store,
        OllamaConfig::from_env().unwrap(),
        CrawlAccess {
            allowed_sources: ["fixture".to_owned()].into(),
            allow_private_network: true,
            browser_fallback: false,
        },
        listener,
        shutdown.clone(),
    ));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let (first, second) = tokio::join!(initialize(&client, &url), initialize(&client, &url));
    assert_ne!(first, second);
    let list = json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_sources","arguments":{}}});
    let (a, b) = tokio::join!(
        rpc(&client, &url, Some(&first), list.clone()),
        rpc(&client, &url, Some(&second), list.clone())
    );
    for response in [a.1, b.1] {
        assert!(response.get("error").is_none(), "{response}");
        assert!(
            response["result"].to_string().contains("Fixture docs"),
            "{response}"
        );
    }
    let response = rpc(&client, &url, Some(&first), json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"crawl","arguments":{"source_id":"fixture","lexical_only":true,"max_pages":1}}})).await.1;
    assert_ne!(response["result"]["isError"], true, "{response}");
    let job_id = response["result"]["structuredContent"]["job"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    client
        .delete(&url)
        .header("Mcp-Session-Id", &first)
        .header("MCP-Protocol-Version", "2025-03-26")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let response = rpc(&client, &url, Some(&second), list).await.1;
    assert!(response["result"].to_string().contains("Fixture docs"));
    // The first client's background crawl survives its session teardown.
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let response = rpc(&client, &url, Some(&second), json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"job_status","arguments":{"job_id":job_id}}})).await.1;
            let status = response["result"]["structuredContent"]["job"]["status"].as_str().unwrap();
            if status == "completed" { break; }
            assert!(["queued", "running"].contains(&status), "{response}");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.unwrap();
    let response = rpc(&client, &url, Some(&second), json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"search","arguments":{"query":"proxy","source_id":"fixture"}}})).await.1;
    assert!(
        !response["result"]["structuredContent"]["hits"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{response}"
    );
    fixture.abort();
    // The local endpoint rejects browser origins and forged Host authorities.
    for (header, value) in [
        ("Origin", "https://untrusted.example"),
        ("Host", "untrusted.example"),
    ] {
        assert_eq!(
            client
                .post(&url)
                .header(header, value)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
    }
    shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // The SurrealDB SDK shuts down its engine asynchronously after handle drop.
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(store) = KnowledgeStore::open(directory.path()).await {
                break store;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(reopened.get_source("fixture").await.unwrap().is_some());
}

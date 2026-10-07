//! Executable checks against the pinned embedded engine; analyzer behavior is
//! an integration contract, not inferred from examples for another SDK version.
use surrealdb::{
    engine::any::connect,
    opt::{Config, capabilities::Capabilities},
};

#[tokio::test]
async fn punctuation_tokenization_requires_an_exact_identifier_filter() {
    let directory = tempfile::TempDir::new().unwrap();
    let db = connect((
        format!("surrealkv://{}", directory.path().display()),
        Config::new().capabilities(Capabilities::default().with_all_functions_allowed()),
    ))
    .await
    .unwrap();
    db.use_ns("probe").use_db("probe").await.unwrap();
    db.query("DEFINE ANALYZER documentation TOKENIZERS blank, punct FILTERS lowercase; DEFINE INDEX text_search ON entry FIELDS text FULLTEXT ANALYZER documentation BM25; INSERT INTO entry [{name: 'exact', text: 'Use Proxy::custom() for SOCKS5 proxies with ClientBuilder.'}, {name: 'separate', text: 'Proxy supports custom settings through Trait::method and ClientBuilderSuffix.'}, {name: 'prefix', text: 'Proxy::customized() supports something different.'}];").await.unwrap().check().unwrap();
    let mut result = db.query("RETURN search::analyze('documentation', 'Proxy::custom() ClientBuilder SOCKS5'); SELECT VALUE name FROM entry WHERE text @0@ $query ORDER BY name; SELECT VALUE name FROM entry WHERE text @0@ $query AND string::matches(text, $pattern) ORDER BY name; RETURN string::slice('αβγ代理', 1, 3);")
        .bind(("query", "Proxy::custom")).bind(("pattern", r"(?i)(?:^|[^\p{L}\p{N}_])Proxy::custom(?:$|[^\p{L}\p{N}_:])")).await.unwrap().check().unwrap();
    let tokens: Vec<String> = result.take(0).unwrap();
    assert_eq!(
        tokens,
        [
            "proxy",
            ":",
            ":",
            "custom",
            "(",
            ")",
            "clientbuilder",
            "socks5"
        ]
    );
    let broad: Vec<String> = result.take(1).unwrap();
    assert_eq!(broad, ["exact", "separate"]);
    let exact: Vec<String> = result.take(2).unwrap();
    assert_eq!(exact, ["exact"]);
    let slice: Option<String> = result.take(3).unwrap();
    assert_eq!(slice.as_deref(), Some("βγ"));
}

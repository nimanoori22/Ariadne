//! Bounded documentation manifests feed Spider's existing frontier.
use super::CrawlRequest;
use quick_xml::{Reader, events::Event};
use serde::{Deserialize, Serialize};
use spider::{client::Client, website::Website};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::OnceLock,
};
use url::Url;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DiscoveryReport {
    pub attempts: Vec<DiscoveryAttempt>,
    pub seeded_urls: usize,
    pub capped: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DiscoveryAttempt {
    pub url: String,
    pub status: Option<u16>,
    pub outcome: String,
    pub retained_bytes: usize,
}

pub(super) async fn discover(
    client: &Client,
    website: &Website,
    request: &CrawlRequest,
) -> (BTreeSet<String>, DiscoveryReport) {
    let mut report = DiscoveryReport::default();
    let mut urls = BTreeSet::new();
    let mut queue: VecDeque<_> = ["llms.txt", "llms-full.txt", "sitemap.xml"]
        .into_iter()
        .filter_map(|name| request.seed.join(name).ok())
        .filter(|u| request.scope.contains(u))
        .collect();
    let cap = (request.max_pages as usize).min(1000);
    let mut seen = BTreeSet::new();
    while let Some(url) = queue.pop_front() {
        if report.attempts.len() >= 6 || urls.len() >= cap {
            report.capped = true;
            break;
        }
        if !seen.insert(url.to_string()) {
            continue;
        }
        let mut attempt = DiscoveryAttempt {
            url: url.to_string(),
            status: None,
            outcome: "failed".into(),
            retained_bytes: 0,
        };
        if !request.scope.contains(&url) || !website.is_allowed_robots(url.as_str()) {
            attempt.outcome = "blocked".into();
            report.attempts.push(attempt);
            continue;
        }
        let fetched = async {
            let mut response = client.get(url.clone()).send().await?;
            attempt.status = Some(response.status().as_u16());
            if !response.status().is_success() {
                attempt.outcome = "http_failure".into();
                return Ok::<_, anyhow::Error>(None);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if bytes.len() + chunk.len() > 512 * 1024 {
                    attempt.outcome = "too_large".into();
                    return Ok(None);
                }
                bytes.extend_from_slice(&chunk);
            }
            attempt.retained_bytes = bytes.len();
            Ok(Some(bytes))
        }
        .await;
        if let Err(error) = &fetched {
            tracing::warn!(url=%url,error=%error,"manifest request failed");
        }
        if let Ok(Some(bytes)) = fetched {
            let parsed = if url.path().ends_with(".xml") {
                sitemap_links(&bytes, &url)
            } else {
                markdown_links(&bytes, &url).map(|links| (links, vec![]))
            };
            match parsed {
                Ok((links, maps)) => {
                    attempt.outcome = "parsed".into();
                    for mut link in links {
                        link.set_fragment(None);
                        if request.scope.contains(&link)
                            && website.is_allowed_robots(link.as_str())
                            && request
                                .max_path_segments
                                .is_none_or(|max| super::path_segments(&link) <= max)
                        {
                            if urls.len() < cap {
                                urls.insert(link.to_string());
                            } else {
                                report.capped = true;
                            }
                        }
                    }
                    for map in maps {
                        if request.scope.contains(&map) && queue.len() < 6 {
                            queue.push_back(map);
                        } else {
                            report.capped = true;
                        }
                    }
                }
                Err(_) => attempt.outcome = "parse_failure".into(),
            }
        }
        report.attempts.push(attempt);
        // Reuse Spider's interpreted robots crawl delay for manifest requests.
        if website.configuration.delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(
                website.configuration.delay,
            ))
            .await;
        }
    }
    report.seeded_urls = urls.len();
    (urls, report)
}

fn markdown_links(bytes: &[u8], base: &Url) -> anyhow::Result<Vec<Url>> {
    static LINKS: OnceLock<regex::Regex> = OnceLock::new();
    let text = std::str::from_utf8(bytes)?;
    let regex =
        LINKS.get_or_init(|| regex::Regex::new(r"\[[^\]\n]*\]\(([^\s)]+)(?:\s+[^)]*)?\)").unwrap());
    Ok(regex
        .captures_iter(text)
        .take(1000)
        .filter_map(|c| base.join(c[1].trim_matches(['<', '>'])).ok())
        .collect())
}
fn sitemap_links(bytes: &[u8], base: &Url) -> anyhow::Result<(Vec<Url>, Vec<Url>)> {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut links = Vec::new();
    let mut maps = Vec::new();
    let mut loc = String::new();
    loop {
        match reader.read_event()? {
            Event::Start(tag) => {
                anyhow::ensure!(stack.len() < 64, "sitemap depth exceeded");
                if stack.is_empty() {
                    anyhow::ensure!(
                        matches!(tag.local_name().as_ref(), b"urlset" | b"sitemapindex"),
                        "invalid sitemap root"
                    );
                }
                stack.push(tag.local_name().as_ref().to_vec());
                if stack.last().is_some_and(|n| n == b"loc") {
                    loc.clear();
                }
            }
            Event::Text(text) if stack.last().is_some_and(|n| n == b"loc") => {
                loc.push_str(&quick_xml::escape::unescape(&text.decode()?)?)
            }
            Event::GeneralRef(reference) if stack.last().is_some_and(|n| n == b"loc") => {
                let escaped = format!("&{};", reference.decode()?);
                loc.push_str(&quick_xml::escape::unescape(&escaped)?);
            }
            Event::CData(text) if stack.last().is_some_and(|n| n == b"loc") => {
                loc.push_str(&text.decode()?)
            }
            Event::End(_) => {
                if stack.last().is_some_and(|n| n == b"loc")
                    && stack.len() >= 2
                    && !loc.trim().is_empty()
                    && let Ok(url) = base.join(loc.trim())
                {
                    if stack[stack.len() - 2] == b"sitemap" {
                        maps.push(url);
                    } else if stack[stack.len() - 2] == b"url" {
                        links.push(url);
                    }
                }
                stack.pop();
                anyhow::ensure!(
                    links.len() + maps.len() <= 1000,
                    "sitemap URL limit exceeded"
                );
            }
            Event::DocType(_) => anyhow::bail!("sitemap DTDs are unsupported"),
            Event::Eof => break,
            _ => {}
        }
    }
    anyhow::ensure!(stack.is_empty(), "incomplete sitemap");
    Ok((links, maps))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespaced_sitemaps_preserve_entities_and_reject_dtds() {
        let base = Url::parse("https://example.com/docs/sitemap.xml").unwrap();
        let (links,maps)=sitemap_links(br#"<s:urlset xmlns:s="urn:sitemap"><s:url><s:loc>page?a=1&amp;b=2</s:loc></s:url><s:url><s:loc></s:loc></s:url></s:urlset>"#,&base).unwrap();
        assert!(maps.is_empty());
        assert_eq!(links[0].as_str(), "https://example.com/docs/page?a=1&b=2");
        assert_eq!(links.len(), 1);
        let (_, maps) = sitemap_links(
            b"<sitemapindex><sitemap><loc>child.xml</loc></sitemap></sitemapindex>",
            &base,
        )
        .unwrap();
        assert_eq!(maps[0].path(), "/docs/child.xml");
        assert!(sitemap_links(b"<!DOCTYPE a><urlset/>", &base).is_err());
        assert!(sitemap_links(b"<html><loc>page</loc></html>", &base).is_err());
        assert!(sitemap_links(b"<urlset><url>", &base).is_err());
    }
    #[test]
    fn markdown_manifest_uses_page_urls_without_fragments_as_content() {
        let base = Url::parse("https://example.com/docs/llms.txt").unwrap();
        let links = markdown_links(
            b"# Docs\n[API](api#methods)\n[Other](https://other.test/)",
            &base,
        )
        .unwrap();
        assert_eq!(links[0].as_str(), "https://example.com/docs/api#methods");
        assert_eq!(links.len(), 2);
    }
}

//! Deterministic HTML-to-document extraction. Source text is untrusted data.
mod model;
mod render;
pub use model::*;

use crate::crawler::{PageOutcome, PageState};
use scraper::{ElementRef, Html, Selector};
use url::Url;

pub const EXTRACTION_VERSION: &str = "html-structure-v1";
const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_DEPTH: usize = 128;

fn selector(value: &str) -> Selector {
    Selector::parse(value).expect("static selector")
}

/// Rejections retain the original page, including unsuccessful fetch outcomes.
pub fn extract(page: PageOutcome) -> ExtractionOutcome {
    match extract_html(&page) {
        Ok(parts) => ExtractionOutcome::Extracted(Box::new(ExtractedDocument {
            page,
            extraction_version: EXTRACTION_VERSION.into(),
            title: parts.title,
            canonical_url: parts.canonical_url,
            declared_canonical_url: parts.declared_canonical_url,
            sections: parts.sections,
            links: parts.links,
            diagnostics: parts.diagnostics,
            quality: parts.quality,
        })),
        Err(reason) => ExtractionOutcome::Rejected {
            page: Box::new(page),
            reason,
        },
    }
}

struct DocumentParts {
    title: String,
    canonical_url: Url,
    declared_canonical_url: Option<Url>,
    sections: Vec<Section>,
    links: Vec<DocumentLink>,
    diagnostics: Vec<Diagnostic>,
    quality: ExtractionQuality,
}

fn extract_html(page: &PageOutcome) -> Result<DocumentParts, ExtractionFailure> {
    if page.state != PageState::Fetched
        || !(200..300).contains(&page.status)
        || page.content_truncated
    {
        return Err(ExtractionFailure::FetchNotSuccessful);
    }
    if page.raw_body.len() > MAX_INPUT_BYTES {
        return Err(ExtractionFailure::InputTooLarge);
    }
    if let Some((_, value)) = page
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
    {
        let value = String::from_utf8_lossy(value).to_ascii_lowercase();
        let media_type = value.split(';').next().unwrap_or_default().trim();
        if !matches!(media_type, "text/html" | "application/xhtml+xml") {
            return Err(ExtractionFailure::UnsupportedContentType);
        }
        if value.split(';').skip(1).any(|part| {
            part.trim().strip_prefix("charset=").is_some_and(|charset| {
                !matches!(
                    charset.trim_matches(['\'', '"']).trim(),
                    "utf-8" | "utf8" | "us-ascii"
                )
            })
        }) {
            return Err(ExtractionFailure::UnsupportedEncoding);
        }
    }
    let html = std::str::from_utf8(&page.raw_body).map_err(|_| ExtractionFailure::InvalidUtf8)?;
    let mut final_url =
        Url::parse(&page.final_url).map_err(|_| ExtractionFailure::InvalidSourceUrl)?;
    if !matches!(final_url.scheme(), "http" | "https")
        || !final_url.username().is_empty()
        || final_url.password().is_some()
    {
        return Err(ExtractionFailure::InvalidSourceUrl);
    }
    final_url.set_fragment(None);
    let dom = Html::parse_document(html);
    let mut diagnostics = Vec::new();
    let mut base = final_url.clone();
    if let Some(element) = dom.select(&selector("head base[href]")).next() {
        match safe_join(&final_url, element.attr("href").unwrap_or_default()) {
            Some(url) => base = url,
            None => diagnostics.push(Diagnostic::InvalidBaseUrl),
        }
    }
    let declared_canonical_url = dom
        .select(&selector("head link[href]"))
        .find(|element| {
            element.attr("rel").is_some_and(|rel| {
                rel.split_whitespace()
                    .any(|part| part.eq_ignore_ascii_case("canonical"))
            })
        })
        .and_then(|element| {
            let url = safe_join(&base, element.attr("href").unwrap_or_default());
            if url.is_none() {
                diagnostics.push(Diagnostic::InvalidCanonicalUrl);
            }
            url
        });
    let mut roots: Vec<_> = dom
        .select(&selector("main, [role=main]"))
        .filter(|element| visible(*element))
        .collect();
    if roots.is_empty() {
        roots = dom
            .select(&selector("article"))
            .filter(|element| visible(*element))
            .collect();
    }
    if roots.len() > 1 {
        diagnostics.push(Diagnostic::MultipleContentRoots);
    }
    let root = match roots.first().copied() {
        Some(root) => root,
        None => {
            diagnostics.push(Diagnostic::BodyFallback);
            dom.select(&selector("body"))
                .next()
                .ok_or(ExtractionFailure::EmptyContent)?
        }
    };
    let mut builder = Builder {
        base,
        links: Vec::new(),
        diagnostics,
        too_deep: false,
    };
    let mut events = Vec::new();
    builder.walk(root, &mut events, 0);
    if builder.too_deep {
        return Err(ExtractionFailure::StructureTooDeep);
    }
    let mut sections = vec![Section {
        id: 0,
        parent_id: None,
        heading: None,
        heading_level: 0,
        anchor: None,
        blocks: Vec::new(),
    }];
    let mut stack = vec![0];
    for event in events {
        match event {
            Event::Heading {
                text,
                level,
                anchor,
            } => {
                while stack.len() > 1 && sections[*stack.last().unwrap()].heading_level >= level {
                    stack.pop();
                }
                let id = sections.len();
                sections.push(Section {
                    id,
                    parent_id: stack.last().copied(),
                    heading: Some(text),
                    heading_level: level,
                    anchor,
                    blocks: Vec::new(),
                });
                stack.push(id);
            }
            Event::Block(block) => sections[*stack.last().unwrap()].blocks.push(block),
        }
    }
    let title = dom
        .select(&selector("head title"))
        .next()
        .map(|element| normalized(&element.text().collect::<String>()))
        .filter(|title| !title.is_empty())
        .or_else(|| {
            sections
                .iter()
                .find(|section| section.heading_level == 1)
                .and_then(|section| section.heading.clone())
        })
        .unwrap_or_else(|| {
            builder.diagnostics.push(Diagnostic::MissingTitle);
            final_url.to_string()
        });
    let mut document = DocumentParts {
        title,
        canonical_url: final_url,
        declared_canonical_url,
        sections,
        links: builder.links,
        diagnostics: builder.diagnostics,
        quality: ExtractionQuality::Useful,
    };
    let count = render::sections_text(&document.sections)
        .chars()
        .filter(|character| !character.is_whitespace())
        .count();
    if count == 0 {
        return Err(ExtractionFailure::EmptyContent);
    }
    if count < 40 {
        document.quality = ExtractionQuality::LowContent;
        document.diagnostics.push(Diagnostic::SparseContent);
    }
    Ok(document)
}

fn normalized(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
fn safe_join(base: &Url, href: &str) -> Option<Url> {
    base.join(href.trim()).ok().filter(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none()
    })
}
fn note_kind(element: ElementRef<'_>) -> Option<String> {
    element
        .attr("class")
        .unwrap_or_default()
        .split_whitespace()
        .find(|class| {
            matches!(
                *class,
                "note" | "warning" | "tip" | "important" | "caution" | "admonition"
            )
        })
        .map(str::to_owned)
        .or_else(|| (element.attr("role") == Some("note")).then(|| "note".into()))
}
fn excluded(element: ElementRef<'_>) -> bool {
    let name = element.value().name();
    element.attr("hidden").is_some()
        || element
            .attr("aria-hidden")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        || matches!(
            name,
            "script"
                | "style"
                | "template"
                | "noscript"
                | "nav"
                | "footer"
                | "form"
                | "button"
                | "input"
                | "select"
                | "textarea"
                | "svg"
                | "canvas"
        )
        || (name == "aside" && note_kind(element).is_none())
        || (name == "header"
            && element
                .parent()
                .and_then(ElementRef::wrap)
                .is_some_and(|parent| parent.value().name() == "body"))
        || matches!(element.attr("role"), Some("navigation" | "banner"))
        || element
            .attr("class")
            .unwrap_or_default()
            .split_whitespace()
            .any(|class| matches!(class, "sidebar" | "cookie-banner" | "cookie-consent"))
}
fn visible(element: ElementRef<'_>) -> bool {
    !excluded(element)
        && !element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(excluded)
}

enum Event {
    Heading {
        text: String,
        level: u8,
        anchor: Option<String>,
    },
    Block(ContentBlock),
}
struct Builder {
    base: Url,
    links: Vec<DocumentLink>,
    diagnostics: Vec<Diagnostic>,
    too_deep: bool,
}

impl Builder {
    fn walk(&mut self, element: ElementRef<'_>, events: &mut Vec<Event>, depth: usize) {
        if depth > MAX_DEPTH {
            self.too_deep = true;
            return;
        }
        if excluded(element) {
            return;
        }
        let name = element.value().name();
        if let Some(level) = name
            .strip_prefix('h')
            .and_then(|level| level.parse::<u8>().ok())
            .filter(|level| (1..=6).contains(level))
        {
            let content = self.inline(element, depth + 1);
            let text = render::inline_text(&content).trim().to_owned();
            if !text.is_empty() {
                let anchor = element.attr("id").map(str::to_owned).or_else(|| {
                    element
                        .select(&selector("a[id], a[name]"))
                        .next()
                        .and_then(|a| a.attr("id").or_else(|| a.attr("name")).map(str::to_owned))
                });
                events.push(Event::Heading {
                    text,
                    level,
                    anchor,
                });
            }
            return;
        }
        let block =
            if name == "pre" {
                let code = element.select(&selector("code")).next().unwrap_or(element);
                let language = code
                    .attr("class")
                    .or_else(|| element.attr("class"))
                    .unwrap_or_default()
                    .split_whitespace()
                    .find_map(|class| {
                        class
                            .strip_prefix("language-")
                            .or_else(|| class.strip_prefix("lang-"))
                    })
                    .map(str::to_owned);
                Some(ContentBlock::Code {
                    text: code.text().collect(),
                    language,
                })
            } else if matches!(name, "p" | "dt" | "dd") {
                let content = self.inline(element, depth + 1);
                (!render::inline_text(&content).trim().is_empty())
                    .then_some(ContentBlock::Paragraph(content))
            } else if matches!(name, "ul" | "ol") {
                let mut items = Vec::new();
                for item in element
                    .children()
                    .filter_map(ElementRef::wrap)
                    .filter(|item| item.value().name() == "li" && !excluded(*item))
                {
                    items.push(self.nested(item, depth + 1));
                }
                Some(ContentBlock::List {
                    ordered: name == "ol",
                    start: element
                        .attr("start")
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(1),
                    items,
                })
            } else if name == "table" {
                let caption = element
                    .select(&selector("caption"))
                    .find(|caption| visible(*caption))
                    .map(|caption| normalized(&caption.text().collect::<String>()));
                let mut rows = Vec::new();
                for row in element.select(&selector("tr")).filter(|row| {
                    visible(*row)
                        && row
                            .ancestors()
                            .filter_map(ElementRef::wrap)
                            .find(|parent| parent.value().name() == "table")
                            == Some(element)
                }) {
                    let mut cells = Vec::new();
                    for cell in row.children().filter_map(ElementRef::wrap).filter(|cell| {
                        matches!(cell.value().name(), "th" | "td") && !excluded(*cell)
                    }) {
                        cells.push(TableCell {
                            content: self.inline(cell, depth + 1),
                            header: cell.value().name() == "th",
                            colspan: span(cell, "colspan"),
                            rowspan: span(cell, "rowspan"),
                        });
                    }
                    if !cells.is_empty() {
                        rows.push(cells);
                    }
                }
                Some(ContentBlock::Table { caption, rows })
            } else if name == "blockquote" {
                Some(ContentBlock::Quote(self.nested(element, depth + 1)))
            } else {
                note_kind(element).map(|kind| ContentBlock::Note {
                    kind,
                    blocks: self.nested(element, depth + 1),
                })
            };
        if let Some(block) = block {
            events.push(Event::Block(block));
            return;
        }
        self.children(element, events, depth + 1);
    }

    fn nested(&mut self, element: ElementRef<'_>, depth: usize) -> Vec<ContentBlock> {
        let mut events = Vec::new();
        self.children(element, &mut events, depth);
        events
            .into_iter()
            .map(|event| match event {
                Event::Block(block) => block,
                Event::Heading { text, .. } => {
                    ContentBlock::Paragraph(vec![Inline::Strong(vec![Inline::Text(text)])])
                }
            })
            .collect()
    }

    fn children(&mut self, element: ElementRef<'_>, events: &mut Vec<Event>, depth: usize) {
        let mut pending = Vec::new();
        for node in element.children() {
            if let Some(text) = node.value().as_text() {
                pending.push(Inline::Text(collapse_whitespace(text)));
            } else if let Some(child) = ElementRef::wrap(node) {
                if excluded(child) {
                    continue;
                }
                if matches!(
                    child.value().name(),
                    "a" | "code" | "em" | "i" | "strong" | "b" | "span" | "br" | "img"
                ) {
                    pending.extend(self.inline_element(child, depth));
                } else {
                    flush(&mut pending, events);
                    self.walk(child, events, depth);
                }
            }
        }
        flush(&mut pending, events);
    }

    fn inline(&mut self, element: ElementRef<'_>, depth: usize) -> Vec<Inline> {
        if depth > MAX_DEPTH {
            self.too_deep = true;
            return Vec::new();
        }
        let mut content = Vec::new();
        for node in element.children() {
            if let Some(text) = node.value().as_text() {
                content.push(Inline::Text(collapse_whitespace(text)));
            } else if let Some(child) = ElementRef::wrap(node) {
                if excluded(child) {
                    continue;
                }
                let block_boundary = matches!(child.value().name(), "p" | "div" | "li");
                if block_boundary {
                    content.push(Inline::Break);
                }
                content.extend(self.inline_element(child, depth + 1));
                if block_boundary {
                    content.push(Inline::Break);
                }
            }
        }
        content
    }

    fn inline_element(&mut self, element: ElementRef<'_>, depth: usize) -> Vec<Inline> {
        if depth > MAX_DEPTH {
            self.too_deep = true;
            return Vec::new();
        }
        match element.value().name() {
            "br" => vec![Inline::Break],
            "img" => element
                .attr("alt")
                .map(|alt| vec![Inline::Text(alt.into())])
                .unwrap_or_default(),
            "code" => vec![Inline::Code(element.text().collect())],
            "em" | "i" => vec![Inline::Emphasis(self.inline(element, depth + 1))],
            "strong" | "b" => vec![Inline::Strong(self.inline(element, depth + 1))],
            "a" if element.attr("href").is_some() => {
                let href = element.attr("href").unwrap().to_owned();
                let url = safe_join(&self.base, &href);
                if url.is_none() {
                    self.diagnostics.push(Diagnostic::InvalidLink(href.clone()));
                }
                let content = self.inline(element, depth + 1);
                self.links.push(DocumentLink {
                    text: render::inline_text(&content).trim().into(),
                    href: href.clone(),
                    url: url.clone(),
                });
                vec![Inline::Link { content, href, url }]
            }
            _ => self.inline(element, depth + 1),
        }
    }
}

fn collapse_whitespace(text: &str) -> String {
    let mut result = String::new();
    let mut whitespace = false;
    for character in text.chars() {
        if character.is_whitespace() {
            if !whitespace {
                result.push(' ');
            }
            whitespace = true;
        } else {
            result.push(character);
            whitespace = false;
        }
    }
    result
}
fn flush(content: &mut Vec<Inline>, events: &mut Vec<Event>) {
    if !render::inline_text(content).trim().is_empty() {
        events.push(Event::Block(ContentBlock::Paragraph(std::mem::take(
            content,
        ))));
    } else {
        content.clear();
    }
}
fn span(element: ElementRef<'_>, name: &str) -> usize {
    element
        .attr(name)
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

/// Conservative upgrade signal: scripts plus an empty application root or an
/// explicit loading/JavaScript placeholder. Short static API pages stay HTTP.
pub(crate) fn browser_candidate(page: &PageOutcome) -> bool {
    if page.state != PageState::Fetched || page.status != 200 || page.content_truncated {
        return false;
    }
    let Ok(text) = std::str::from_utf8(&page.raw_body) else {
        return false;
    };
    let dom = Html::parse_document(text);
    let scripts = dom.select(&selector("script")).any(|s| {
        !matches!(
            s.attr("type"),
            Some("application/ld+json" | "application/json")
        )
    });
    if !scripts {
        return false;
    }
    match extract_html(page) {
        Err(ExtractionFailure::EmptyContent) => true,
        Ok(parts) => {
            let body = render::sections_text(&parts.sections).trim().to_lowercase();
            let placeholder = body.len() < 200
                && ["loading", "enable javascript", "javascript is required"]
                    .iter()
                    .any(|s| body.contains(s));
            let empty_app = dom
                .select(&selector("#app, #root, #__next, [data-reactroot]"))
                .next()
                .is_some()
                && parts.sections.iter().all(|s| s.blocks.is_empty());
            placeholder || empty_app
        }
        _ => false,
    }
}

#[cfg(test)]
mod browser_tests {
    use super::*;
    fn page(body: &str) -> PageOutcome {
        PageOutcome {
            source_id: "docs".into(),
            crawl_id: "test".into(),
            requested_url: "https://example.com/docs/".into(),
            final_url: "https://example.com/docs/".into(),
            fetched_at: std::time::SystemTime::now(),
            status: 200,
            headers: vec![("content-type".into(), b"text/html".to_vec())],
            raw_body: body.as_bytes().to_vec(),
            content_truncated: false,
            rendering: None,
            state: PageState::Fetched,
        }
    }
    #[test]
    fn upgrade_signals_preserve_short_static_and_failed_pages() {
        assert!(browser_candidate(&page(
            "<html><main id='app'></main><script src='app.js'></script></html>"
        )));
        assert!(browser_candidate(&page(
            "<html><main><p>Loading documentation...</p></main><script src='app.js'></script></html>"
        )));
        assert!(!browser_candidate(&page(
            "<html><main><h1>API</h1><pre><code>Client::new()</code></pre></main><script src='tracking.js'></script></html>"
        )));
        assert!(!browser_candidate(&page(
            "<html><main><p>A short static API page.</p></main></html>"
        )));
        let mut failed = page("<html><main id='app'></main><script></script></html>");
        failed.status = 403;
        assert!(!browser_candidate(&failed));
        failed.status = 200;
        failed.headers[0].1 = b"application/json".to_vec();
        assert!(!browser_candidate(&failed));
    }
}

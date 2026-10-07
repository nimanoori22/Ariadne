use serde::{Deserialize, Serialize};
use url::Url;

use crate::crawler::PageOutcome;

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractedDocument {
    /// Original bytes and all fetch provenance remain available for reprocessing.
    pub page: PageOutcome,
    pub extraction_version: String,
    pub title: String,
    pub canonical_url: Url,
    /// A source-provided hint; never silently replaces the fetched URL.
    pub declared_canonical_url: Option<Url>,
    pub sections: Vec<Section>,
    pub links: Vec<DocumentLink>,
    pub diagnostics: Vec<Diagnostic>,
    pub quality: ExtractionQuality,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section {
    /// Document-local ID and order. Zero is the synthetic introduction section.
    pub id: usize,
    pub parent_id: Option<usize>,
    pub heading: Option<String>,
    pub heading_level: u8,
    pub anchor: Option<String>,
    pub blocks: Vec<ContentBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Inline {
    Text(String),
    Code(String),
    Emphasis(Vec<Inline>),
    Strong(Vec<Inline>),
    Link {
        content: Vec<Inline>,
        href: String,
        url: Option<Url>,
    },
    Break,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentBlock {
    Paragraph(Vec<Inline>),
    Code {
        text: String,
        language: Option<String>,
    },
    List {
        ordered: bool,
        start: i64,
        items: Vec<Vec<ContentBlock>>,
    },
    Table {
        caption: Option<String>,
        rows: Vec<Vec<TableCell>>,
    },
    Note {
        kind: String,
        blocks: Vec<ContentBlock>,
    },
    Quote(Vec<ContentBlock>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCell {
    pub content: Vec<Inline>,
    pub header: bool,
    pub colspan: usize,
    pub rowspan: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentLink {
    pub text: String,
    pub href: String,
    pub url: Option<Url>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Diagnostic {
    BodyFallback,
    MissingTitle,
    SparseContent,
    InvalidLink(String),
    InvalidBaseUrl,
    InvalidCanonicalUrl,
    MultipleContentRoots,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtractionQuality {
    Useful,
    LowContent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtractionFailure {
    FetchNotSuccessful,
    UnsupportedContentType,
    UnsupportedEncoding,
    InvalidUtf8,
    InvalidSourceUrl,
    InputTooLarge,
    StructureTooDeep,
    EmptyContent,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExtractionOutcome {
    Extracted(Box<ExtractedDocument>),
    Rejected {
        page: Box<PageOutcome>,
        reason: ExtractionFailure,
    },
}

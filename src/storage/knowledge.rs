//! Bounded agent-facing document reads; raw bodies remain an inspection concern.
use super::*;
use crate::chunking::Chunk;
impl KnowledgeStore {
    pub async fn list_sources_page(&self, after: Option<&str>, limit: usize) -> Result<Value> {
        ensure!(
            (1..=100).contains(&limit),
            "source page limit must be 1..100"
        );
        ensure!(
            after.is_none_or(|s| s.len() <= 1024),
            "invalid source cursor"
        );
        let mut result=self.db.query("SELECT VALUE data FROM source WHERE data.id > $after ORDER BY data.id LIMIT $limit;")
            .bind(("after",after.unwrap_or_default().to_owned())).bind(("limit",limit+1)).await?.check()?;
        let mut sources: Vec<Value> = result.take(0)?;
        let more = sources.len() > limit;
        sources.truncate(limit);
        let cursor = if more {
            sources
                .last()
                .and_then(|s| s["id"].as_str())
                .map(str::to_owned)
        } else {
            None
        };
        Ok(json!({"sources":sources,"next_cursor":cursor}))
    }
    pub async fn knowledge_document(
        &self,
        source: &str,
        url: &Url,
        section: Option<usize>,
        max_chars: usize,
    ) -> Result<Option<Value>> {
        ensure!(
            !source.trim().is_empty() && source.len() <= 1024 && (1..=20000).contains(&max_chars),
            "invalid document request"
        );
        let mut url = url.clone();
        url.set_fragment(None);
        let mut result=self.db.query("BEGIN TRANSACTION; LET $doc=type::record('document',[$source,$url]); SELECT VALUE {revision_id:current_revision,title:data.title,extraction_version:data.extraction_version,indexing:data.indexing,revalidation:{availability:data.revalidation.availability,last_checked_at:data.revalidation.last_checked_at,status:data.revalidation.status}} FROM $doc; SELECT VALUE object::extend(data,{text:string::slice(data.text,0,$max_chars),markdown:'',char_count:string::len(data.text)}) FROM chunk WHERE document=$doc AND ($section=NONE OR $section=NULL OR data.section_id=$section) ORDER BY sequence LIMIT 101; SELECT VALUE {id:data.id,parent_id:data.parent_id,heading:data.heading,heading_level:data.heading_level,anchor:data.anchor} FROM section WHERE document=$doc AND ($section=NONE OR $section=NULL OR data.id=$section) ORDER BY sequence LIMIT 101; COMMIT TRANSACTION;")
            .bind(("source",source.to_owned())).bind(("url",url.to_string())).bind(("section",section)).bind(("max_chars",max_chars)).await?.check()?;
        let docs: Vec<Value> = result.take(2)?;
        let Some(metadata) = docs.into_iter().next() else {
            return Ok(None);
        };
        let chunks: Vec<Value> = result.take(3)?;
        let sections: Vec<Value> = result.take(4)?;
        render_document(source, &url, section, max_chars, metadata, chunks, sections)
    }
}

pub(super) fn render_document(
    source: &str,
    url: &Url,
    section: Option<usize>,
    max_chars: usize,
    metadata: Value,
    chunks: Vec<Value>,
    sections: Vec<Value>,
) -> Result<Option<Value>> {
    if section.is_some() && sections.is_empty() {
        return Ok(None);
    }
    let mut remaining = max_chars;
    let mut pieces = Vec::new();
    for chunk in chunks.iter().take(100) {
        let chunk: Chunk = serde_json::from_value(chunk.clone())?;
        if remaining == 0 {
            break;
        }
        let count = chunk.char_count;
        let text: String = chunk.text.chars().take(remaining).collect();
        remaining -= text.chars().count();
        pieces.push(json!({"content_kind":"source_data","chunk_id":chunk.id,"revision_id":metadata["revision_id"],"text":text,"text_truncated":count>text.chars().count(),"section_id":chunk.section_id,"heading_path":chunk.heading_path,"url":chunk.source_url,"content_sha256":chunk.content_sha256,"crawl_id":chunk.crawl_id,"crawled_at":chunk.crawled_at,"sequence":chunk.sequence}));
    }
    // Heading-only and removed sections remain inspectable without a raw-body
    // read. Their text is source data and availability remains explicit.
    let headings:Vec<Value>=sections.iter().take(100).map(|s|json!({"id":s["id"],"parent_id":s["parent_id"],"heading":s["heading"],"heading_level":s["heading_level"],"anchor":s["anchor"]})).collect();
    let truncated = chunks.len() > pieces.len()
        || sections.len() > 100
        || pieces.iter().any(|p| p["text_truncated"] == true);
    Ok(Some(
        json!({"content_kind":"source_data","source_id":source,"document_url":url,"metadata":metadata,"sections":headings,"chunks":pieces,"text_chars":max_chars-remaining,"truncated":truncated}),
    ))
}

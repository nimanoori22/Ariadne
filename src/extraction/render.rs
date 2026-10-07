use super::{ContentBlock, ExtractedDocument, Inline, Section};

impl ExtractedDocument {
    /// Derived interchange representation; sections and blocks remain the source of truth.
    pub fn markdown(&self) -> String {
        self.sections
            .iter()
            .flat_map(|section| {
                let mut parts = Vec::new();
                if let Some(heading) = &section.heading {
                    parts.push(format!(
                        "{} {}",
                        "#".repeat(section.heading_level as usize),
                        escape(heading)
                    ));
                }
                parts.extend(section.blocks.iter().map(block_markdown));
                parts
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    pub fn plain_text(&self) -> String {
        sections_text(&self.sections)
    }
}

impl ContentBlock {
    pub fn markdown(&self) -> String {
        block_markdown(self)
    }
    pub fn plain_text(&self) -> String {
        block_text(self)
    }
}

pub(super) fn sections_text(sections: &[Section]) -> String {
    sections
        .iter()
        .flat_map(|section| {
            let mut parts = Vec::new();
            if let Some(heading) = &section.heading {
                parts.push(heading.clone());
            }
            parts.extend(section.blocks.iter().map(block_text));
            parts
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub(super) fn inline_text(content: &[Inline]) -> String {
    content
        .iter()
        .map(|item| match item {
            Inline::Text(text) | Inline::Code(text) => text.clone(),
            Inline::Emphasis(content) | Inline::Strong(content) | Inline::Link { content, .. } => {
                inline_text(content)
            }
            Inline::Break => "\n".into(),
        })
        .collect()
}

fn inline_markdown(content: &[Inline]) -> String {
    content
        .iter()
        .map(|item| match item {
            Inline::Text(text) => escape(text),
            Inline::Code(text) => {
                let fence = "`".repeat(longest_ticks(text) + 1);
                format!("{fence} {text} {fence}")
            }
            Inline::Emphasis(content) => format!("*{}*", inline_markdown(content)),
            Inline::Strong(content) => format!("**{}**", inline_markdown(content)),
            Inline::Link {
                content,
                url: Some(url),
                ..
            } => format!("[{}](<{}>)", inline_markdown(content), url),
            Inline::Link {
                content, url: None, ..
            } => inline_markdown(content),
            Inline::Break => "  \n".into(),
        })
        .collect()
}

fn block_markdown(block: &ContentBlock) -> String {
    match block {
        ContentBlock::Paragraph(content) => inline_markdown(content).trim().into(),
        ContentBlock::Code { text, language } => {
            let fence = "`".repeat((longest_ticks(text) + 1).max(3));
            let language = language
                .as_deref()
                .unwrap_or_default()
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '+'))
                .collect::<String>();
            format!(
                "{fence}{language}\n{text}{}{fence}",
                if text.ends_with('\n') { "" } else { "\n" }
            )
        }
        ContentBlock::List {
            ordered,
            start,
            items,
        } => items
            .iter()
            .enumerate()
            .map(|(index, blocks)| {
                let marker = if *ordered {
                    format!("{}.", start.saturating_add(index as i64))
                } else {
                    "-".into()
                };
                let text = blocks
                    .iter()
                    .map(block_markdown)
                    .collect::<Vec<_>>()
                    .join("\n\n");
                let indent = " ".repeat(marker.len() + 1);
                let mut lines = text.lines();
                let first = lines.next().unwrap_or_default();
                let continuation = lines
                    .map(|line| format!("\n{indent}{line}"))
                    .collect::<String>();
                format!("{marker} {first}{continuation}")
            })
            .collect::<Vec<_>>()
            .join("\n"),
        ContentBlock::Table { caption, rows } => {
            let width = rows.iter().map(Vec::len).max().unwrap_or(0);
            if width == 0 {
                return caption.clone().unwrap_or_default();
            }
            let row_text = |row: &[super::TableCell]| {
                let values = (0..width)
                    .map(|index| {
                        row.get(index)
                            .map(|cell| {
                                inline_markdown(&cell.content)
                                    .trim()
                                    .replace('|', "\\|")
                                    .replace("  \n", "<br>")
                            })
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>();
                format!("| {} |", values.join(" | "))
            };
            let has_header = rows[0].iter().any(|cell| cell.header);
            let mut lines = vec![
                if has_header {
                    row_text(&rows[0])
                } else {
                    format!("| {} |", vec![""; width].join(" | "))
                },
                format!("| {} |", vec!["---"; width].join(" | ")),
            ];
            lines.extend(
                rows.iter()
                    .skip(usize::from(has_header))
                    .map(|row| row_text(row)),
            );
            if let Some(caption) = caption {
                lines.insert(0, format!("{}\n", escape(caption)));
            }
            lines.join("\n")
        }
        ContentBlock::Note { kind, blocks } => quote(&format!(
            "[!{}]\n{}",
            kind.to_ascii_uppercase(),
            blocks
                .iter()
                .map(block_markdown)
                .collect::<Vec<_>>()
                .join("\n\n")
        )),
        ContentBlock::Quote(blocks) => quote(
            &blocks
                .iter()
                .map(block_markdown)
                .collect::<Vec<_>>()
                .join("\n\n"),
        ),
    }
}

fn block_text(block: &ContentBlock) -> String {
    match block {
        ContentBlock::Paragraph(content) => inline_text(content).trim().into(),
        ContentBlock::Code { text, .. } => text.clone(),
        ContentBlock::List { items, .. } => items
            .iter()
            .map(|blocks| blocks.iter().map(block_text).collect::<Vec<_>>().join("\n"))
            .collect::<Vec<_>>()
            .join("\n"),
        ContentBlock::Table { caption, rows } => caption
            .iter()
            .cloned()
            .chain(rows.iter().map(|row| {
                row.iter()
                    .map(|cell| inline_text(&cell.content).trim().to_owned())
                    .collect::<Vec<_>>()
                    .join("\t")
            }))
            .collect::<Vec<_>>()
            .join("\n"),
        ContentBlock::Note { blocks, .. } | ContentBlock::Quote(blocks) => blocks
            .iter()
            .map(block_text)
            .collect::<Vec<_>>()
            .join("\n\n"),
    }
}

fn quote(text: &str) -> String {
    text.lines()
        .map(|line| format!("> {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
fn longest_ticks(text: &str) -> usize {
    text.split(|character| character != '`')
        .map(str::len)
        .max()
        .unwrap_or(0)
}
fn escape(text: &str) -> String {
    text.chars()
        .flat_map(|character| {
            if "\\`*_{}[]<>#".contains(character) {
                vec!['\\', character]
            } else {
                vec![character]
            }
        })
        .collect()
}

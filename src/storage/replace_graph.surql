DELETE links_to WHERE in = $document;
DELETE mentions WHERE document = $document;
FOR $link IN $doc.graph.links {
    LET $target = type::record('document', [$source_id, $link.target_url]);
    LET $edge = type::record('links_to', [$source_id, $doc.url, $link.url]);
    RELATE $document->$edge->$target SET data = object::extend($link, {
        source_id: $source_id, document_url: $doc.url, title: string::slice($doc.data.title,0,200),
        title_truncated: string::len($doc.data.title)>200,
        crawl_id: $doc.data.page.crawl_id, crawled_at: $doc.data.page.fetched_at,
        version: $doc.graph.version
    });
};
FOR $mention IN $doc.graph.mentions {
    LET $entity = type::record('entity', ['rust_path', $mention.symbol]);
    UPSERT $entity SET symbol = $mention.symbol, kind = 'rust_path';
    LET $chunk = type::record('chunk', $mention.chunk_id);
    LET $edge = type::record('mentions', [$mention.chunk_id, $mention.symbol]);
    RELATE $chunk->$edge->$entity SET document = $document, data = object::extend($mention, {
        source_id: $source_id, document_url: $doc.url, title: string::slice($doc.data.title,0,200),
        title_truncated: string::len($doc.data.title)>200,
        heading_path: array::map(array::slice($chunk.data.heading_path,0,16), |$h| string::slice($h,0,200)),
        heading_path_truncated: array::len($chunk.data.heading_path)>16 OR array::len(array::filter($chunk.data.heading_path, |$h| string::len($h)>200))>0,
        crawl_id: $chunk.data.crawl_id,
        crawled_at: $chunk.data.crawled_at, content_sha256: $chunk.data.content_sha256,
        version: $doc.graph.version
    });
};
UPDATE $document SET data.graph = {version: $doc.graph.version,
    link_count: array::len($doc.graph.links), mention_count: array::len($doc.graph.mentions),
    truncated: $doc.graph.truncated};

BEGIN TRANSACTION;
LET $run = type::record('crawl_run', [$source_id, $crawl_id]);
IF $run.status != 'running' { THROW 'crawl is not running'; };
FOR $index IN 0..array::len($pages) {
    CREATE type::record('page_outcome', [$source_id, $crawl_id, $index])
        SET crawl = $run, data = $pages[$index];
};
FOR $doc IN $documents {
    LET $document = type::record('document', [$source_id, $doc.url]);
    -- Retain unchanged vectors; obsolete derivatives leave the search index
    -- atomically with their chunks. Stale metadata remains inspectable.
    FOR $table IN (SELECT VALUE table_name FROM embedding_space) {
        UPDATE type::table($table) SET data.status = 'stale', vector = NONE, data.updated_at = $finished
            WHERE document = $document AND data.chunk_id NOT IN $doc.chunks.*.id;
    };
    DELETE chunk WHERE document = $document;
    DELETE section WHERE document = $document;
    UPSERT $document SET source = type::record('source', $source_id), crawl = $run,
        canonical_url = $doc.url, data = $doc.data;
    UPDATE $document SET data.revalidation = $doc.validation ?? {availability: 'active', last_checked_at: $finished,
        last_checked_crawl_id: $crawl_id, status: $doc.data.page.status, headers: $doc.data.page.headers};
    FOR $section IN $doc.sections {
        CREATE type::record('section', [$source_id, $doc.url, $section.id])
            SET document = $document, sequence = $section.id, data = $section,
                parent = IF $section.parent_id = NULL { NONE }
                    ELSE { type::record('section', [$source_id, $doc.url, $section.parent_id]) };
    };
    FOR $chunk IN $doc.chunks {
        CREATE type::record('chunk', $chunk.id)
            SET document = $document,
                section = type::record('section', [$source_id, $doc.url, $chunk.section_id]),
                sequence = $chunk.sequence, data = $chunk;
    };
    -- GRAPH_REPLACE
};
FOR $reuse IN $reused {
    LET $document = type::record('document', [$source_id, $reuse.url]);
    IF !record::exists($document) OR $document.data.indexing.hashes.normalized_sha256 != $reuse.indexing.hashes.normalized_sha256 {
        THROW 'reused document changed during recrawl';
    };
    LET $headers = IF $reuse.kind = 'not_modified' {
        -- Merge only representation/cache metadata from the 304, never its
        -- zero Content-Length into the retained HTML representation.
        LET $base = $document.data.revalidation.headers ?? $document.data.page.headers;
        LET $fresh = array::filter($reuse.page.headers, |$h| $h[0] IN ['etag', 'last-modified', 'cache-control', 'vary', 'content-location']);
        LET $names = array::map($fresh, |$h| $h[0]);
        array::concat(array::filter($base, |$h| $h[0] NOT IN $names), $fresh)
    } ELSE { $reuse.page.headers };
    LET $graph = $document.data.graph;
    IF $reuse.refreshed != NULL { UPDATE $document SET data = $reuse.refreshed, data.graph = $graph; }
    ELSE IF $reuse.kind != 'not_modified' { UPDATE $document SET data.page = $reuse.page; };
    UPDATE $document SET data.indexing = $reuse.indexing,
        data.revalidation = {availability: 'active', last_checked_at: $finished,
            last_checked_crawl_id: $crawl_id, status: $reuse.page.status, headers: $headers};
};
FOR $removed IN $removed {
    LET $document = type::record('document', [$source_id, $removed.url]);
    IF !record::exists($document) { THROW 'removed document is unknown'; };
    FOR $table IN (SELECT VALUE table_name FROM embedding_space) {
        UPDATE type::table($table) SET data.status = 'stale', vector = NONE, data.updated_at = $finished WHERE document = $document;
    };
    DELETE chunk WHERE document = $document;
    DELETE links_to WHERE in = $document;
    DELETE mentions WHERE document = $document;
    -- Keep raw/structured content for inspection and future reprocessing, but
    -- remove retrieval units only after a directly observed 404/410.
    UPDATE $document SET data.revalidation = {availability: 'removed',
        last_checked_at: $finished, last_checked_crawl_id: $crawl_id,
        status: $removed.page.status, headers: []};
    UPDATE $document SET data.graph = NONE;
};
UPDATE $run SET status = 'completed', data.status = 'completed',
    data.finished_at = $finished, data.summary = $summary;
UPDATE $run SET data.summary.budget_reached = array::len($pages) >= data.configuration.max_pages;
COMMIT TRANSACTION;

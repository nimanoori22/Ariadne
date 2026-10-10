BEGIN TRANSACTION;
LET $run = type::record('crawl_run', [$source_id, $crawl_id]);
IF $run.status != 'running' { THROW 'crawl is not running'; };
LET $offset = IF $site = NULL { 0 } ELSE { $run.data.summary.page_count ?? 0 };
FOR $index IN 0..array::len($pages) {
    CREATE type::record('page_outcome', [$source_id, $crawl_id, $offset + $index])
        SET crawl = $run, data = $pages[$index];
};
FOR $doc IN $documents {
    LET $document = type::record('document', [$source_id, $doc.url]);
    -- ARCHIVE_REVISION
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
    -- ARCHIVE_REVISION
};
FOR $reuse IN $reused {
    LET $document = type::record('document', [$source_id, $reuse.url]);
    IF !record::exists($document) OR $document.data.indexing.hashes.normalized_sha256 != $reuse.indexing.hashes.normalized_sha256 {
        THROW 'reused document changed during recrawl';
    };
    -- ARCHIVE_REVISION
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
    -- ARCHIVE_REVISION
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
IF $site != NULL {
    FOR $url IN $site.links {
        LET $ref=type::record('crawl_url',[$source_id,$crawl_id,$url]);
        IF !record::exists($ref) {CREATE $ref SET crawl=$run,url=$url,status='pending';};
    };
    FOR $visit IN $site.visited {
        UPDATE type::record('crawl_url',[$source_id,$crawl_id,$visit.url]) SET status=$visit.status,reason=$visit.reason ?? NONE;
    };
    LET $previous=$run.data.summary ?? {};
    UPDATE $run SET data.summary = object::extend($summary,{
        page_count:($previous.page_count ?? 0)+$summary.page_count,
        document_count:($previous.document_count ?? 0)+$summary.document_count,
        chunk_count:($previous.chunk_count ?? 0)+$summary.chunk_count,
        rejected_count:($previous.rejected_count ?? 0)+$summary.rejected_count,
        oversized_chunk_count:($previous.oversized_chunk_count ?? 0)+$summary.oversized_chunk_count,
        alias_count:($previous.alias_count ?? 0)+$summary.alias_count,
        retained_body_bytes:($previous.retained_body_bytes ?? 0)+$summary.retained_body_bytes,
        crawl_elapsed_ms:($previous.crawl_elapsed_ms ?? 0)+$summary.crawl_elapsed_ms,
        started_at:($previous.started_at ?? $summary.started_at),
        discovery:IF array::len($summary.discovery.attempts)>0 {$summary.discovery} ELSE {$previous.discovery ?? $summary.discovery},
        delivery_complete:($previous.delivery_complete ?? true) AND $summary.delivery_complete,
        budget_reached:false,crawl_mode:'all_html',frontier_exhausted:$site.finish,
        batches:($previous.batches ?? 0)+ IF array::len($pages)>0 {1} ELSE {0},
        last_checkpoint_at:$finished,
        frontier:{
            pending:((SELECT count() AS n FROM crawl_url WHERE crawl=$run AND status='pending' GROUP ALL)[0].n ?? 0),
            done:((SELECT count() AS n FROM crawl_url WHERE crawl=$run AND status='done' GROUP ALL)[0].n ?? 0),
            failed:((SELECT count() AS n FROM crawl_url WHERE crawl=$run AND status='failed' GROUP ALL)[0].n ?? 0),
            blocked:((SELECT count() AS n FROM crawl_url WHERE crawl=$run AND status='blocked' GROUP ALL)[0].n ?? 0),
            skipped:((SELECT count() AS n FROM crawl_url WHERE crawl=$run AND status='skipped' GROUP ALL)[0].n ?? 0)
        }
    });
    IF $site.finish {
        UPDATE $run SET status='completed',data.status='completed',data.finished_at=$finished;
    };
} ELSE {
    UPDATE $run SET status = 'completed', data.status = 'completed',
        data.finished_at = $finished, data.summary = $summary;
    UPDATE $run SET data.summary.budget_reached = array::len($pages) >= data.configuration.max_pages;
};
COMMIT TRANSACTION;

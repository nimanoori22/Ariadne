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
};
UPDATE $run SET status = 'completed', data.status = 'completed',
    data.finished_at = $finished, data.summary = $summary;
COMMIT TRANSACTION;

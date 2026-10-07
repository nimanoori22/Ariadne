BEGIN TRANSACTION;
RETURN $hits.map(|$hit| {
    LET $matched = type::record('chunk', $hit.id);
    IF !record::exists($matched) OR $matched.data.content_sha256 != $hit.hash
        OR $matched.data.source_id != $hit.source OR $matched.data.document_url != $hit.url { [] }
    ELSE {
        -- Fetch bounded neighbors in the same document, across section boundaries.
        -- Keep metadata/hash of the complete chunk, but cap text at the boundary.
        SELECT VALUE {chunk: object::extend(data, {
            text: string::slice(data.text, 0, $max_chars), markdown: '',
        }), text_truncated: string::len(data.text) > $max_chars} FROM chunk WITH INDEX chunks_by_document
        WHERE document = $matched.document
            AND sequence >= math::max([$matched.sequence - $radius, 0])
            AND sequence <= $matched.sequence + $radius
        ORDER BY sequence LIMIT 7
    }
});
COMMIT TRANSACTION;

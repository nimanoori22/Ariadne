-- HIT_PROJECTION
, $path AS graph_path FROM chunk WHERE id IN (
    SELECT VALUE in FROM mentions WHERE out=$symbol AND document != $document
        AND data.source_id=$seed.source AND data.version=$version
        AND document.source=$document.source AND record::exists(in)
        AND data.content_sha256=in.data.content_sha256
        AND (document.data.revalidation.availability ?? 'active') != 'removed'
        AND in.data.source_id=$seed.source
        AND ($source_id='' OR in.data.source_id=$source_id)
        -- RELATION_METADATA_FILTER
    ORDER BY in.data.document_url, in.sequence LIMIT $chunk_limit
)
    AND ($source_id='' OR data.source_id=$source_id)
    -- METADATA_FILTER
ORDER BY data.document_url, sequence LIMIT $chunk_limit

-- HIT_PROJECTION
, $path AS graph_path FROM chunk WHERE document=$target
    AND ($anchor=NONE OR $anchor=NULL OR $anchor='' OR data.anchor=$anchor OR parse::url::fragment(data.source_url)=$anchor)
    AND ($source_id='' OR data.source_id=$source_id)
    -- METADATA_FILTER
ORDER BY sequence LIMIT $chunk_limit

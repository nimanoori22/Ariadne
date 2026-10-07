FROM chunk WITH INDEX chunk_title_search
WHERE data.title @0@ $query
    AND ($source_id = '' OR data.source_id = $source_id)
    AND ($exact_pattern = '' OR string::matches(data.title, $exact_pattern))
ORDER BY score DESC, source_id ASC, document_url ASC, sequence ASC, chunk_id ASC
LIMIT $limit TIMEOUT 10s;

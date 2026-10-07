FROM chunk WITH INDEX chunk_text_search
WHERE data.text @0@ $query
    AND ($source_id = '' OR data.source_id = $source_id)
    AND ($exact_pattern = '' OR string::matches(data.text, $exact_pattern))
ORDER BY score DESC, source_id ASC, document_url ASC, sequence ASC, chunk_id ASC
LIMIT $limit TIMEOUT 10s;

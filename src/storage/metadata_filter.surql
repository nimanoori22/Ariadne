AND ($url_prefix = '' OR data.document_url = $url_prefix
    OR string::starts_with(data.document_url, $url_subtree)
    OR string::starts_with(data.document_url, $url_query))
AND ($heading = '' OR array::any(data.heading_path, |$h| string::lowercase($h) = $heading))
AND ($crawled_after = NULL OR data.crawled_at.secs_since_epoch >= $crawled_after)

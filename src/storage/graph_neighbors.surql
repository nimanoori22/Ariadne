BEGIN TRANSACTION;
RETURN $seeds.map(|$seed| {
    LET $matched = type::record('chunk', $seed.id);
    LET $document = $matched.document;
    IF !record::exists($matched) OR $matched.data.content_sha256 != $seed.hash
        OR $matched.data.source_id != $seed.source OR $matched.data.document_url != $seed.url
        OR ($document.data.revalidation.availability ?? 'active') = 'removed' {
        {status:'stale'}
    } ELSE IF $document.data.graph.version != $version {
        {status:'unindexed'}
    } ELSE {
        LET $edges = IF $links {
            SELECT VALUE {target: IF in=$document {out} ELSE {in},
                relation: IF in=$document {'outgoing_link'} ELSE {'incoming_link'},
                anchor: IF in=$document {parse::url::fragment(data.url)} ELSE {NONE},
                url: data.url, source_url: data.document_url}
            FROM links_to WHERE (in=$document OR out=$document) AND in != out
                AND data.version=$version AND record::exists(in) AND record::exists(out)
                AND in.source=$document.source AND out.source=$document.source
                AND (in.data.revalidation.availability ?? 'active') != 'removed'
                AND (out.data.revalidation.availability ?? 'active') != 'removed'
            ORDER BY data.document_url, data.url LIMIT $edge_limit
        } ELSE { [] };
        LET $symbols = IF $entities {
            SELECT VALUE out FROM mentions WHERE in=$matched AND document=$document AND data.version=$version
                AND data.content_sha256=$seed.hash
                ORDER BY data.symbol LIMIT $edge_limit
        } ELSE { [] };
        LET $link_groups = array::slice($edges,0,$max_edges).map(|$edge| {
            LET $path = {relation:$edge.relation, seed_chunk_id:$seed.id, seed_content_sha256:$seed.hash,
                seed_url:$seed.source_url, link_source_url:$edge.source_url, link_url:$edge.url, entity:NULL};
            LET $target = $edge.target;
            LET $anchor = $edge.anchor;
            -- LINK_CANDIDATES
        });
        LET $entity_groups = array::slice($symbols,0,$max_edges).map(|$symbol| {
            LET $path = {relation:'shared_entity', seed_chunk_id:$seed.id, seed_content_sha256:$seed.hash,
                seed_url:$seed.source_url, link_source_url:NULL, link_url:NULL, entity:$symbol.symbol};
            -- ENTITY_CANDIDATES
        });
        {status:'ready', index_truncated:$document.data.graph.truncated ?? false,
            edges_truncated:array::len($edges)>$max_edges OR array::len($symbols)>$max_edges,
            groups:array::concat($link_groups,$entity_groups)}
    }
});
COMMIT TRANSACTION;

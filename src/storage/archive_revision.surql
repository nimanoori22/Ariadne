-- Called before destructive replacement/removal and after installing new chunks.
-- Identity excludes transport metadata and crawl times. Include all processing
-- versions and chunk policy, so reprocessing can create a new representation.
IF record::exists($document) {
    LET $meta = $document.data.indexing;
    LET $revision_id = IF $meta = NONE OR $meta = NULL {
        -- Very early stores have structured/raw documents but no chunk metadata.
        -- Preserve them as inspectable legacy snapshots, with zero search units.
        crypto::sha256(type::string(['legacy-raw-v1', $source_id, $document.canonical_url, $document.data.extraction_version, $document.data.page.raw_body]))
    } ELSE { crypto::sha256(type::string([
        'indexed-representation-v1', $source_id, $document.canonical_url,
        $meta.hashes.normalized_sha256, $meta.extraction_version,
        $meta.hashes.normalization_version, $meta.chunking_version, $meta.policy.target_chars
    ])) };
    LET $revision = type::record('document_revision', $revision_id);
    IF !record::exists($revision) {
        LET $sections = SELECT VALUE data FROM section WHERE document = $document ORDER BY sequence;
        LET $chunks = SELECT VALUE data FROM chunk WHERE document = $document ORDER BY sequence;
        LET $sequence = ($document.revision_sequence ?? 0) + 1;
        CREATE $revision SET document = $document, revision_id = $revision_id,
            sequence = $sequence, stored_at = $finished, data = $document.data,
            section_count = array::len($sections), chunk_count = array::len($chunks);
        FOR $section IN $sections {
            CREATE type::record('revision_section', [$revision_id, $section.id])
                SET revision = $revision, sequence = $section.id, data = $section;
        };
        FOR $chunk IN $chunks {
            CREATE type::record('revision_chunk', [$revision_id, $chunk.id])
                SET revision = $revision, document = $document, sequence = $chunk.sequence, data = $chunk;
        };
        UPDATE $document SET revision_sequence = $sequence;
    };
    UPDATE $document SET current_revision = $revision_id;
};

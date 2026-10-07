BEGIN TRANSACTION;
LET $chunk = type::record('chunk', $chunk_id);
LET $embedding = type::record($table, $chunk_id);
IF !record::exists($chunk) OR $chunk.data.content_sha256 != $hash { THROW 'chunk changed before embedding preparation'; };
LET $reuse = $embedding.data.status = 'ready' AND $embedding.data.content_sha256 = $hash AND $embedding.vector != NONE;
IF !$reuse {
    UPSERT $embedding SET chunk = $chunk, document = $chunk.document,
        source_id = $chunk.data.source_id, vector = NONE,
        data = {chunk_id: $chunk_id, content_sha256: $hash, status: 'pending',
            attempts: ($embedding.data.attempts ?? 0) + 1, created_at: NULL,
            updated_at: $now, error: NULL};
};
COMMIT TRANSACTION;
RETURN !$reuse;

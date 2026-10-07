BEGIN TRANSACTION;
LET $chunk = type::record('chunk', $chunk_id);
LET $embedding = type::record($table, $chunk_id);
LET $stale = !record::exists($chunk) OR $chunk.data.content_sha256 != $hash;
LET $ready = $embedding.data.status = 'ready';
IF $stale {
    UPDATE $embedding SET data.status = 'stale', vector = NONE, data.updated_at = $now;
} ELSE IF $ready {
    -- A concurrent successful producer owns the valid result; failures cannot
    -- replace it. Generation is at-least-once, persistence is idempotent.
} ELSE {
    UPDATE $embedding SET vector = IF $success { $vector } ELSE { NONE },
        data.status = IF $success { 'ready' } ELSE { 'failed' },
        data.created_at = IF $success { $now } ELSE { NULL },
        data.updated_at = $now, data.error = $error;
};
COMMIT TRANSACTION;
RETURN !$stale AND !$ready;

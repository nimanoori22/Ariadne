BEGIN TRANSACTION;
LET $record = type::record('source', $source_id);
IF record::exists($record) {
    IF $record.data.name != $data.name OR $record.data.root_url != $data.root_url {
        THROW 'source ID already registered with different metadata';
    };
} ELSE {
    CREATE $record SET data = $data;
};
COMMIT TRANSACTION;

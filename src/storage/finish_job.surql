BEGIN TRANSACTION;
LET $job=type::record('crawl_job',$id);
IF $job.status NOT IN ['queued','running'] {THROW 'job is already terminal';};
LET $run=type::record('crawl_run',[$job.data.source_id,$job.data.crawl_id]);
IF record::exists($run) {
    IF $run.status='running' AND $status IN ['cancelled','failed'] {
        UPDATE $run SET status=$status,data.status=$status,data.error=$error,data.finished_at=$now;
    };
    IF $status IN ['cancelled','failed','partial'] AND $run.data.ingestion != NONE AND $run.data.ingestion != NULL AND $run.data.ingestion.status='running' {
        UPDATE $run SET data.ingestion.status=$status,data.ingestion.error=$error,
            data.ingestion.updated_at=$now,data.ingestion.finished_at=$now;
    };
};
UPDATE $job SET status=$status,data.status=$status,data.error=$error,
    data.finished_at=$now,data.elapsed_ms=$elapsed;
COMMIT TRANSACTION;

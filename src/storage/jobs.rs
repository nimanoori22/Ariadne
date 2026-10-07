use super::*;
use crate::jobs::{JobRecord, JobSnapshot, JobStatus};

impl KnowledgeStore {
    pub(crate) async fn reserve_job(&self, job: &JobRecord) -> Result<()> {
        self.transaction("BEGIN TRANSACTION; IF !record::exists(type::record('source',$source)) {THROW 'unknown source';}; UPDATE type::record('source',$source) SET admission_revision=(admission_revision ?? 0)+1; IF array::len(SELECT id FROM crawl_job WHERE source = type::record('source',$source) AND status IN ['queued','running'] LIMIT 1)>0 OR array::len(SELECT id FROM crawl_run WHERE source = type::record('source',$source) AND status = 'running' LIMIT 1)>0 {THROW 'source already has an active job';}; CREATE ONLY type::record('crawl_job',$id) SET source=type::record('source',$source),status='queued',data=$data; COMMIT TRANSACTION;",json!({"source":job.source_id,"id":job.id,"data":job})).await?;
        Ok(())
    }
    pub(crate) async fn start_job(&self, id: &str) -> Result<()> {
        self.transaction("BEGIN TRANSACTION; LET $job=type::record('crawl_job',$id); IF $job.status!='queued' {THROW 'job is not queued';}; UPDATE $job SET status='running',data.status='running'; COMMIT TRANSACTION;",json!({"id":id})).await?;
        Ok(())
    }
    pub(crate) async fn finish_job(
        &self,
        id: &str,
        status: JobStatus,
        error: Option<String>,
    ) -> Result<()> {
        let now = SystemTime::now();
        let snapshot = self.get_job(id).await?.context("job not found")?;
        let elapsed = now
            .duration_since(snapshot.job.created_at)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        self.transaction(
            include_str!("finish_job.surql"),
            json!({"id":id,"status":status,"error":error,"now":now,"elapsed":elapsed}),
        )
        .await?;
        Ok(())
    }
    pub async fn get_job(&self, id: &str) -> Result<Option<JobSnapshot>> {
        ensure!(
            !id.is_empty()
                && id.len() <= 128
                && id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "invalid job ID"
        );
        let mut result = self
            .db
            .query("SELECT VALUE data FROM type::record('crawl_job',$id);")
            .bind(("id", id.to_owned()))
            .await?
            .check()?;
        let jobs: Vec<Value> = result.take(0)?;
        let Some(value) = jobs.into_iter().next() else {
            return Ok(None);
        };
        let job: JobRecord = serde_json::from_value(value)?;
        let run = self.get_crawl(&job.source_id, &job.crawl_id).await?;
        Ok(Some(JobSnapshot { job, run }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JobOperation;
    fn job(id: &str, status: JobStatus) -> JobRecord {
        JobRecord {
            id: id.into(),
            source_id: "docs".into(),
            crawl_id: id.into(),
            operation: JobOperation::Crawl,
            status,
            created_at: SystemTime::now(),
            finished_at: None,
            elapsed_ms: None,
            configuration: json!({}),
            error: None,
        }
    }
    #[tokio::test]
    async fn additive_schema_upgrade_recovers_jobs_and_closed_store_backup_restores_them() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old");
        let db = connect((
            format!("surrealkv://{}", old.display()),
            Config::new().capabilities(Capabilities::default().with_all_functions_allowed()),
        ))
        .await
        .unwrap();
        db.use_ns("ariadne").use_db("knowledge").await.unwrap();
        let sql = SCHEMA[0]
            .sql
            .split("DEFINE TABLE crawl_job")
            .next()
            .unwrap()
            .replace(
                "'failed', 'cancelled', 'interrupted'",
                "'failed', 'interrupted'",
            );
        Sync::embedded(&[EmbeddedSchemaFile {
            path: "previous.surql",
            sql: Box::leak(sql.into_boxed_str()),
        }])
        .prune(false)
        .run(&db)
        .await
        .unwrap();
        let source = Source::new(
            "docs",
            "Docs",
            Url::parse("https://example.com/docs/").unwrap(),
        )
        .unwrap();
        db.query("CREATE source:docs SET data=$data;")
            .bind(("data", serde_json::to_value(source).unwrap()))
            .await
            .unwrap()
            .check()
            .unwrap();
        drop(db);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let store = KnowledgeStore::open(&old).await.unwrap();
        store
            .reserve_job(&job("queued", JobStatus::Queued))
            .await
            .unwrap();
        store
            .reserve_job(&job("second", JobStatus::Queued))
            .await
            .unwrap_err();
        store.start_job("queued").await.unwrap();
        let source = store.get_source("docs").await.unwrap().unwrap();
        let request = CrawlRequest::new(
            "docs",
            "queued",
            source.root_url.clone(),
            CrawlScope::new(source.root_url, "/docs").unwrap(),
        );
        store.begin_crawl(&request).await.unwrap();
        drop(store);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let store = KnowledgeStore::open(&old).await.unwrap();
        let recovered = store.get_job("queued").await.unwrap().unwrap();
        assert_eq!(recovered.job.status, JobStatus::Interrupted);
        assert_eq!(recovered.run.unwrap().status, "interrupted");
        assert!(store.get_source("docs").await.unwrap().is_some());
        store
            .reserve_job(&job("cancelled", JobStatus::Queued))
            .await
            .unwrap();
        store
            .finish_job("cancelled", JobStatus::Cancelled, None)
            .await
            .unwrap();
        // A late begin query must not resurrect a cancelled job's crawl.
        let mut request = request;
        request.crawl_id = "cancelled".into();
        assert!(store.begin_crawl(&request).await.is_err());
        drop(store);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        fn copy_tree(from: &Path, to: &Path) {
            std::fs::create_dir_all(to).unwrap();
            for entry in std::fs::read_dir(from).unwrap() {
                let entry = entry.unwrap();
                let destination = to.join(entry.file_name());
                if entry.file_type().unwrap().is_dir() {
                    copy_tree(&entry.path(), &destination);
                } else {
                    std::fs::copy(entry.path(), destination).unwrap();
                }
            }
        }
        let restored = dir.path().join("restored");
        copy_tree(&old, &restored);
        let store = KnowledgeStore::open(&restored).await.unwrap();
        assert_eq!(
            store
                .get_job("cancelled")
                .await
                .unwrap()
                .unwrap()
                .job
                .status,
            JobStatus::Cancelled
        );
        assert_eq!(
            store.get_job("queued").await.unwrap().unwrap().job.status,
            JobStatus::Interrupted
        );
        assert!(store.get_source("docs").await.unwrap().is_some());
    }
}

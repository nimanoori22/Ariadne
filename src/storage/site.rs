//! Durable scheduling and coverage for unbounded-total, bounded-batch crawls.
use super::*;
impl KnowledgeStore {
    pub(crate) async fn seed_site(&self, source: &str, run: &str, url: &Url) -> Result<()> {
        self.transaction("BEGIN TRANSACTION; LET $run=type::record('crawl_run',[$source,$run]); LET $urlref=type::record('crawl_url',[$source,$runid,$url]); IF !record::exists($urlref) {CREATE $urlref SET crawl=$run,url=$url,status='pending';}; UPDATE $run SET data.summary={crawl_mode:'all_html',page_count:0,delivery_complete:true,frontier_exhausted:false}; COMMIT TRANSACTION;",json!({"source":source,"run":run,"runid":run,"url":url})).await?;
        Ok(())
    }
    pub(crate) async fn pending_site_urls(
        &self,
        source: &str,
        run: &str,
        limit: usize,
    ) -> Result<Vec<Url>> {
        ensure!((1..=100).contains(&limit), "invalid site batch size");
        let mut result=self.db.query("SELECT VALUE url FROM crawl_url WITH INDEX frontier_by_crawl WHERE crawl=type::record('crawl_run',[$source,$run]) AND status='pending' ORDER BY url LIMIT $limit;").bind(("source",source.to_owned())).bind(("run",run.to_owned())).bind(("limit",limit)).await?.check()?;
        let urls: Vec<String> = result.take(0)?;
        urls.into_iter()
            .map(|u| Url::parse(&u).map_err(Into::into))
            .collect()
    }
    pub async fn site_coverage(&self, source: &str, run: &str) -> Result<Value> {
        let mut result=self.db.query("SELECT status,count() AS count FROM crawl_url WITH INDEX frontier_by_crawl WHERE crawl=type::record('crawl_run',[$source,$run]) GROUP BY status;").bind(("source",source.to_owned())).bind(("run",run.to_owned())).await?.check()?;
        let rows: Vec<Value> = result.take(0)?;
        let mut counts = json!({"pending":0,"done":0,"failed":0,"blocked":0,"skipped":0});
        for row in rows {
            if let Some(status) = row["status"].as_str() {
                counts[status] = row["count"].clone();
            }
        }
        Ok(counts)
    }
    pub(crate) async fn resume_site(&self, source: &str, run: &str) -> Result<()> {
        self.transaction("BEGIN TRANSACTION; LET $run=type::record('crawl_run',[$source,$run]); IF !record::exists($run) OR $run.data.summary.crawl_mode!='all_html' OR $run.status NOT IN ['interrupted','failed','cancelled'] {THROW 'site crawl cannot resume';}; UPDATE type::record('source',$source) SET admission_revision=(admission_revision ?? 0)+1; IF array::len(SELECT id FROM crawl_run WHERE source=type::record('source',$source) AND status='running' LIMIT 1)>0 OR array::len(SELECT id FROM crawl_job WHERE source=type::record('source',$source) AND status IN ['queued','running'] LIMIT 1)>0 {THROW 'source already active';}; UPDATE $run SET status='running',data.status='running',data.error=NONE,data.finished_at=NULL; COMMIT TRANSACTION;",json!({"source":source,"run":run})).await?;
        Ok(())
    }
}

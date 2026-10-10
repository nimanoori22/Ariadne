DEFINE TABLE source SCHEMAFULL;
DEFINE FIELD data ON source TYPE object FLEXIBLE;
-- Serializes concurrent admission transactions without changing source provenance.
DEFINE FIELD admission_revision ON source TYPE int DEFAULT 0;
DEFINE TABLE crawl_run SCHEMAFULL;
DEFINE FIELD source ON crawl_run TYPE record<source>;
DEFINE FIELD status ON crawl_run TYPE string ASSERT $value IN ['running', 'completed', 'failed', 'interrupted', 'cancelled'];
DEFINE FIELD data ON crawl_run TYPE object FLEXIBLE;
DEFINE TABLE page_outcome SCHEMAFULL;
DEFINE FIELD crawl ON page_outcome TYPE record<crawl_run>;
DEFINE FIELD data ON page_outcome TYPE object FLEXIBLE;
DEFINE INDEX pages_by_crawl ON page_outcome FIELDS crawl;
DEFINE TABLE document SCHEMAFULL;
DEFINE FIELD source ON document TYPE record<source>;
DEFINE FIELD crawl ON document TYPE record<crawl_run>;
DEFINE FIELD canonical_url ON document TYPE string;
DEFINE FIELD data ON document TYPE object FLEXIBLE;
DEFINE INDEX document_identity ON document FIELDS source, canonical_url UNIQUE;
DEFINE TABLE section SCHEMAFULL;
DEFINE FIELD document ON section TYPE record<document>;
DEFINE FIELD parent ON section TYPE option<record<section>>;
DEFINE FIELD sequence ON section TYPE int;
DEFINE FIELD data ON section TYPE object FLEXIBLE;
DEFINE INDEX sections_by_document ON section FIELDS document, sequence UNIQUE;
DEFINE TABLE chunk SCHEMAFULL;
DEFINE FIELD document ON chunk TYPE record<document>;
DEFINE FIELD section ON chunk TYPE record<section>;
DEFINE FIELD sequence ON chunk TYPE int;
DEFINE FIELD data ON chunk TYPE object FLEXIBLE;
DEFINE INDEX chunks_by_document ON chunk FIELDS document, sequence UNIQUE;
DEFINE TABLE embedding_space SCHEMAFULL;
DEFINE FIELD table_name ON embedding_space TYPE string;
DEFINE FIELD data ON embedding_space TYPE object FLEXIBLE;
DEFINE ANALYZER documentation_lexical_v1 TOKENIZERS blank, punct FILTERS lowercase;
DEFINE INDEX chunk_text_search ON chunk FIELDS data.text FULLTEXT ANALYZER documentation_lexical_v1 BM25;
DEFINE INDEX chunk_title_search ON chunk FIELDS data.title FULLTEXT ANALYZER documentation_lexical_v1 BM25;

DEFINE TABLE crawl_job SCHEMAFULL;
DEFINE FIELD source ON crawl_job TYPE record<source>;
DEFINE FIELD status ON crawl_job TYPE string ASSERT $value IN ['queued', 'running', 'completed', 'partial', 'failed', 'cancelled', 'interrupted'];
DEFINE FIELD data ON crawl_job TYPE object FLEXIBLE;
DEFINE INDEX jobs_by_source ON crawl_job FIELDS source;

-- Unindexed link targets remain explicit; reads only traverse indexed documents
-- in the same source. No automatic fetch follows these edges.
DEFINE TABLE links_to TYPE RELATION IN document OUT document SCHEMAFULL;
DEFINE FIELD data ON links_to TYPE object FLEXIBLE;
DEFINE INDEX links_from ON links_to FIELDS in;
DEFINE INDEX links_into ON links_to FIELDS out;
DEFINE TABLE entity SCHEMAFULL;
DEFINE FIELD symbol ON entity TYPE string;
DEFINE FIELD kind ON entity TYPE string;
DEFINE TABLE mentions TYPE RELATION IN chunk OUT entity SCHEMAFULL;
DEFINE FIELD document ON mentions TYPE record<document>;
DEFINE FIELD data ON mentions TYPE object FLEXIBLE;
DEFINE INDEX mentions_by_document ON mentions FIELDS document;
DEFINE INDEX mentions_by_entity ON mentions FIELDS out;

-- Immutable indexed representations; current retrieval indexes stay separate.
DEFINE FIELD current_revision ON document TYPE option<string>;
DEFINE FIELD revision_sequence ON document TYPE int DEFAULT 0;
DEFINE TABLE document_revision SCHEMAFULL;
DEFINE FIELD document ON document_revision TYPE record<document>;
DEFINE FIELD revision_id ON document_revision TYPE string;
DEFINE FIELD sequence ON document_revision TYPE int;
DEFINE FIELD stored_at ON document_revision TYPE object FLEXIBLE;
DEFINE FIELD data ON document_revision TYPE object FLEXIBLE;
DEFINE FIELD chunk_count ON document_revision TYPE int;
DEFINE FIELD section_count ON document_revision TYPE int;
DEFINE INDEX revisions_by_document ON document_revision FIELDS document, sequence UNIQUE;
DEFINE TABLE revision_section SCHEMAFULL;
DEFINE FIELD revision ON revision_section TYPE record<document_revision>;
DEFINE FIELD sequence ON revision_section TYPE int;
DEFINE FIELD data ON revision_section TYPE object FLEXIBLE;
DEFINE INDEX sections_by_revision ON revision_section FIELDS revision, sequence UNIQUE;
DEFINE TABLE revision_chunk SCHEMAFULL;
DEFINE FIELD revision ON revision_chunk TYPE record<document_revision>;
DEFINE FIELD document ON revision_chunk TYPE record<document>;
DEFINE FIELD sequence ON revision_chunk TYPE int;
DEFINE FIELD data ON revision_chunk TYPE object FLEXIBLE;
DEFINE INDEX chunks_by_revision ON revision_chunk FIELDS revision, sequence UNIQUE;
DEFINE INDEX revision_text_search ON revision_chunk FIELDS data.text FULLTEXT ANALYZER documentation_lexical_v1 BM25;
DEFINE INDEX revision_title_search ON revision_chunk FIELDS data.title FULLTEXT ANALYZER documentation_lexical_v1 BM25;

-- Durable URL frontier for full-site HTML crawls. A batch commit advances this
-- frontier in the same transaction as its document and audit writes.
DEFINE TABLE crawl_url SCHEMAFULL;
DEFINE FIELD crawl ON crawl_url TYPE record<crawl_run>;
DEFINE FIELD url ON crawl_url TYPE string;
DEFINE FIELD status ON crawl_url TYPE string ASSERT $value IN ['pending','done','failed','blocked','skipped'];
DEFINE FIELD reason ON crawl_url TYPE option<string>;
DEFINE INDEX frontier_by_crawl ON crawl_url FIELDS crawl, status, url;

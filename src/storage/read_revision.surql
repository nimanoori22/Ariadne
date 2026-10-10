BEGIN TRANSACTION;
LET $doc=type::record('document',[$source,$url]);
LET $rev=type::record('document_revision',$revision_id);
SELECT VALUE {revision_id:revision_id,sequence:sequence,stored_at:stored_at,title:data.title,extraction_version:data.extraction_version,indexing:data.indexing,historical:true,is_current:revision_id=$doc.current_revision} FROM $rev WHERE document=$doc;
SELECT VALUE object::extend(data,{text:string::slice(data.text,0,$max_chars),markdown:'',char_count:string::len(data.text)}) FROM revision_chunk WITH INDEX chunks_by_revision WHERE revision=$rev AND $rev.document=$doc AND ($section=NONE OR $section=NULL OR data.section_id=$section) ORDER BY sequence LIMIT 101;
SELECT VALUE {id:data.id,parent_id:data.parent_id,heading:data.heading,heading_level:data.heading_level,anchor:data.anchor} FROM revision_section WITH INDEX sections_by_revision WHERE revision=$rev AND $rev.document=$doc AND ($section=NONE OR $section=NULL OR data.id=$section) ORDER BY sequence LIMIT 101;
COMMIT TRANSACTION;

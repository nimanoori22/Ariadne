SELECT count() AS documents, count(data.graph.version=$version) AS indexed_documents,
    count(data.graph.truncated=true) AS truncated_documents FROM document
    WHERE source=type::record('source',$source)
        AND (data.revalidation.availability ?? 'active') != 'removed' GROUP ALL;

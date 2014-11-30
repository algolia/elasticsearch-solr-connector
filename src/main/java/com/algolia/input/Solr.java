package com.algolia.input;

import java.net.MalformedURLException;

import org.apache.noggit.JSONUtil;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.json.JSONException;
import org.json.JSONObject;

import com.algolia.output.Output;

public class Solr extends Input {
	CommonsHttpSolrServer client;
	private final int QUERY_SIZE = 10000;
	
	public Solr(Output output, String... params) {
		super(output);
		if (params.length != 1) {
			throw new Error("Solr needs 1 parameter (url)");
		}
		try {
			client = new CommonsHttpSolrServer(params[0]);
		} catch (MalformedURLException e) {
			throw new Error(e);
		}
	}

	@Override
	public void enumerate() {
		int offset = 0;
		while (true) {
			SolrQuery query = new SolrQuery();
			query.setQuery("*:*");
			query.setStart(offset);
			query.setRows(QUERY_SIZE);
			QueryResponse response = null;
			
			try {
				response = client.query(query);
			} catch (SolrServerException e) {
				throw new Error(e);
			}
			SolrDocumentList hits = response.getResults();
			if (hits.size() == 0) {
				break;
			}
			offset += hits.size();
			for (int i = 0; i < hits.size(); ++i) {
				SolrDocument hit = hits.get(i);
				String id = hit.getFieldValue("id").toString();
				try {
					
					this.addObject(id, new JSONObject(JSONUtil.toJSON(hit)));
				} catch (JSONException e) {
					throw new IllegalStateException(e);
				}
			}
		}
	}
}



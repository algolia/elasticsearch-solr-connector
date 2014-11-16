package com.algolia;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONException;
import org.json.JSONObject;

import com.algolia.output.Output;

public class Enumerator {
	protected Client client;
	protected Output output;
	protected String indexName;
	private final int SEARCH_SIZE = 10000;

	public Enumerator(String host, int port, String clusterName, String indexName, Output output) {
		Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).put("cluster.name", clusterName).build();
		client = new TransportClient(settings);
		((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(host, port));
		this.output = output;
		this.indexName = indexName;
	}
	
	public void enumerate() throws JSONException {
		SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setScroll("1m").setSize(SEARCH_SIZE).setSearchType(SearchType.SCAN).execute().actionGet();
		while (true) { 
			System.out.println(response.toString());
			for (SearchHit hit : response.getHits().hits()) {
				JSONObject obj = new JSONObject(hit.getSource());
				obj.put("objectID", hit.getId());
				output.addObject(obj);
			}
			Connector.logger.info(String.format("Retrieve %d hits from elasticsearh", response.getHits().totalHits()));
			response = client.prepareSearchScroll(response.getScrollId()).setScroll("1m").execute().actionGet();
			if (response.getHits().getHits().length == 0) {
				break;
			}
		}
		output.flush();
		client.close();
	}
}

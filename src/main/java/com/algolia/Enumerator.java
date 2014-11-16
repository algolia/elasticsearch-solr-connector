package com.algolia;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;

import com.algolia.output.Output;

public class Enumerator {
	protected Client client;
	protected Output output;
	protected String indexName;
	private final int SEARCH_SIZE = 10000;

	public Enumerator(String host, int port, String indexName, Output output) {
		Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).build();
		client = new TransportClient(settings);
		((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(host, port));
		this.output = output;
		this.indexName = indexName;
	}
	
	public void enumerate() {
		int offset = 0;
		SearchResponse response;
		do {
			response = client.prepareSearch(indexName).setFrom(offset).setSize(SEARCH_SIZE).setSearchType(SearchType.SCAN).execute().actionGet();
			for (SearchHit hit : response.getHits().getHits()) {
				JSONObject obj = new JSONObject(hit.getSource());
				output.addObject(obj);
			}
			Connector.logger.info(String.format("Retrieve %d hits from elasticsearh", response.getHits().totalHits()));
		} while (response.getHits().totalHits() > 0);
		output.flush();
		client.close();
	}
}

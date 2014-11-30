package com.algolia.input;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;

import com.algolia.Connector;
import com.algolia.output.Output;

public class Elasticsearch extends Input {
	protected Client client;
	protected String indexName;
	private final int SEARCH_SIZE = 10000;

	public Elasticsearch(Output output, String... params) {
		super(output);
		if (params.length != 4) {
			throw new Error("Elasticsearch needs 4 parameters (URL:PORT:CLUSTER:INDEX");
		}
		String host = params[0];
		int port = Integer.parseInt(params[1]);
		String clusterName = params[2];
		this.indexName = params[3];
		Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).put("cluster.name", clusterName).build();
		client = new TransportClient(settings);
		((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(host, port));
	}
	
	public void enumerate() {
		SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setScroll("1m").setSize(SEARCH_SIZE).setSearchType(SearchType.SCAN).execute().actionGet();
		while (true) { 
			System.out.println(response.toString());
			for (SearchHit hit : response.getHits().hits()) {
				JSONObject obj = new JSONObject(hit.getSource());
				this.addObject(hit.getId(), obj);
			}
			Connector.logger.info(String.format("Retrieve %d hits from elasticsearh", response.getHits().totalHits()));
			response = client.prepareSearchScroll(response.getScrollId()).setScroll("1m").execute().actionGet();
			if (response.getHits().getHits().length == 0) {
				break;
			}
		}
		
	}

	@Override
	public void close() {
		super.close();
		client.close();
	}
}

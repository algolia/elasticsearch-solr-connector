package com.algolia.output;

import java.util.ArrayList;

import org.json.JSONObject;

import com.algolia.Connector;
import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;

public class Pusher extends Output {

	private Index index;
	private ArrayList<JSONObject> batch;
	private final int BATCH_SIZE = 1000;
	
	public Pusher(String appID, String apiKey, String indexName) {
		index = new APIClient(appID, apiKey).initIndex(indexName);
	}
	
	@Override
	public void addObject(JSONObject obj) {
		batch.add(obj);
		if (batch.size() >= BATCH_SIZE) {
			flush();
		}
	}

	@Override
	public void flush() {
		if (batch.size() > 0) {
			try {
				Connector.logger.info(String.format("Send %d object to algolia", batch.size()));
				index.addObjects(batch);
			} catch (AlgoliaException e) {
				throw new Error(e);
			}
			batch.clear();
		}
	}

	@Override
	public void close() {
		index = null;
	}

}

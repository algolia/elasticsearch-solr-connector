package com.algolia;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;

public class SimpleTest {

    protected final static String CLUSTER = "test-cluster-" + NetworkUtils.getLocalAddress().getHostName();

    private static Node node;
    private static Client client;
    private static APIClient algolia;
    private static Index algolaIndex;
    private static String applicationID;
    private static String apiKey;

	public SimpleTest() {
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		applicationID = System.getenv("ALGOLIA_APPLICATION_ID");
		apiKey = System.getenv("ALGOLIA_API_KEY");
		Assume.assumeFalse("You must set environement variables ALGOLIA_APPLICATION_ID and ALGOLIA_API_KEY to run the tests.", applicationID == null || apiKey == null);
		algolia = new APIClient(applicationID, apiKey);
		algolaIndex = algolia.initIndex("algolia");
		ImmutableSettings.Builder finalSettings = settingsBuilder()
                .put("cluster.name", CLUSTER)
                .put("discovery.zen.ping.multicast.enabled", true)
                .put("node.local", false)
                //.put("gateway.type", "none")
        		.put("cluster.routing.allocation.disk.threshold_enabled", false);
        node = nodeBuilder().settings(finalSettings.put("node.name", "node1").build()).build().start();
        client = node.client();
        if (client.admin().indices().exists(new IndicesExistsRequest("algolia")).actionGet().isExists()) {
			client.admin().indices().delete(new DeleteIndexRequest("algolia")).actionGet();
			node.stop();
			node.start();
		}
        indexElements();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (client.admin().indices().exists(new IndicesExistsRequest("algolia")).actionGet().isExists()) {
			client.admin().indices().delete(new DeleteIndexRequest("algolia")).actionGet();	
		}
		node.close();
	}
	
	public static void indexElements() {
		client.admin().indices().prepareCreate("algolia").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        client.prepareIndex("algolia", "type", "doc0").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("algolia", "type", "doc1").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("algolia", "type", "doc2").setSource("field0", "bar").setRefresh(true).execute().actionGet();
	}

	@Test
	public void test() throws JSONException {
		
       Connector.main(new String[]{"-url", "localhost", "--port", "9300", "--cluster", CLUSTER, "--index", "algolia", "-u", applicationID, "-p", apiKey});
       try {
    	   Thread.sleep(2000); // Wait indexing
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	    try {
			JSONObject response = algolaIndex.browse(0);
			assertEquals(3, response.getInt("nbHits"));
			assertEquals("{\"objectID\":\"doc2\",\"field0\":\"bar\"}", response.getJSONArray("hits").get(0).toString());
			assertEquals("{\"objectID\":\"doc1\",\"field0\":\"foo\"}", response.getJSONArray("hits").get(1).toString());
			assertEquals("{\"objectID\":\"doc0\",\"field0\":\"foo\"}", response.getJSONArray("hits").get(2).toString());
		} catch (AlgoliaException e) {
			e.printStackTrace();
			assertFalse(true);
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}
	}

}

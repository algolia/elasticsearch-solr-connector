package com.algolia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.Test;

import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;

public class SolrTest {
	
	private JettySolrRunner solrRunner;
    private static APIClient algolia;
    private static Index algolaIndex;
    private static String applicationID;
    private static String apiKey;
    private CommonsHttpSolrServer server;

	public SolrTest() throws Exception {
		applicationID = System.getenv("ALGOLIA_APPLICATION_ID");
		apiKey = System.getenv("ALGOLIA_API_KEY");
		Assume.assumeFalse("You must set environement variables ALGOLIA_APPLICATION_ID and ALGOLIA_API_KEY to run the tests.", applicationID == null || apiKey == null);
		algolia = new APIClient(applicationID, apiKey);
		algolaIndex = algolia.initIndex("algolia_solr");
		
		server = new CommonsHttpSolrServer("http://localhost:8983/solr");
	    
		index();
	}
	
	public void index() throws SolrServerException, IOException {
		ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for (int i = 0; i < 100; ++i) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", "doc" + i);
			doc.addField("name", "doc" + i);
			docs.add(doc);
		}
		server.add(docs);
		server.commit(true, true);
	}

	@Test
	public void test() throws JSONException {
		Connector.main(new String[]{"-params", "http://localhost:8983" + "/solr", "-u", applicationID, "-p", apiKey, "-i", "algolia_solr", "--input", "SOLR"});
	       try {
	    	   Thread.sleep(2000); // Wait indexing
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		    try {
				JSONObject response = algolaIndex.browse(0);
				assertEquals(100, response.getInt("nbHits"));
			} catch (AlgoliaException e) {
				e.printStackTrace();
				assertFalse(true);
			} catch (JSONException e) {
				e.printStackTrace();
				assertFalse(true);
			}
	}

}

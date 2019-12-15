package org.dice_research.opal.elastictriples;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.http.HttpHost;
import org.apache.jena.vocabulary.DCAT;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

/**
 * TODO dev code
 *
 * @author Adrian Wilke
 */
public class ElasticsearchTest {

	// Set to true if you have configured Elasticsearch
	public static final boolean EXECUTE_TESTS = true;

	public static String elasticsearchScheme = "http";
	public static String elasticsearchHostname = "localhost";
	public static int elasticsearchPort = 9200;

	public static String elasticsearchIndex = "elastictriples-mcloud";

	private Elasticsearch elasticsearch;
//	HttpHost httpHost;

	@Before
	public void setUp() throws Exception {

		// Test has to be enabled
		Assume.assumeTrue(EXECUTE_TESTS);

		// Ping Elasticsearch
		try {
			elasticsearch = new ElasticsearchImporter()
					.setHttpHost(new HttpHost(elasticsearchHostname, elasticsearchPort, elasticsearchScheme));
			Assume.assumeTrue(elasticsearch.ping());
		} catch (Exception e) {
			System.err.println("Could not connect: " + e.getMessage());
			assumeTrue(false);
		}

//		httpHost = new HttpHost(elasticsearchHostname, elasticsearchPort, elasticsearchScheme);
	}

	@After
	public void tearDown() throws Exception {
		if (elasticsearch != null) {
			elasticsearch.close();
		}
	}

	public void xxxxtest() throws Exception {

		if (Boolean.TRUE) {
			SearchResponse searchResponse = searchObject(DCAT.Distribution.toString());

			System.out.println(DCAT.Distribution.toString());
			System.out.println(searchResponse.getHits().getTotalHits().value);

			for (SearchHit hit : searchResponse.getHits()) {
				System.out.println(hit.getId());
				for (Entry<String, Object> sourceEntry : hit.getSourceAsMap().entrySet()) {
					System.out.println(sourceEntry.getKey());
					System.out.println(sourceEntry.getValue());
				}

				break;
			}
		}

		if (Boolean.FALSE) {
			GetResponse getResponse = getRequest();
			System.out.println(getResponse);
//			String message = getResponse.getField("message").getValue();
		}

	}

	public SearchResponse searchObject(String object) throws IOException {
		try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(elasticsearch.getHttpHost()))) {

			MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("object", object);

			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(matchPhraseQueryBuilder);
			sourceBuilder.size(100);

			SearchRequest searchRequest = new SearchRequest(elasticsearchIndex);
			searchRequest.source(sourceBuilder);

			return client.search(searchRequest, RequestOptions.DEFAULT);
		}
	}

	private GetResponse getRequest() throws IOException {
		try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(elasticsearch.getHttpHost()))) {

			GetRequest getRequest = new GetRequest(elasticsearchIndex, "B5zI_24BpjpH7vYGNXux");
			return client.get(getRequest, RequestOptions.DEFAULT);
		}
	}

}
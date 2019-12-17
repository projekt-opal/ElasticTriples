package org.dice_research.opal.elastictriples;

import static org.junit.Assume.assumeTrue;

import java.util.LinkedList;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.jena.rdf.model.Model;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link ElasticsearchQuery}.
 *
 * @author Adrian Wilke
 */
public class ElasticsearchQueryTest {

	// Set to true if you have configured Elasticsearch
	public static final boolean EXECUTE_TESTS = false;
	public static final boolean EXECUTE_QUERY_DATASETS = false;
	public static final boolean EXECUTE_QUERY_DATASET_GRAPH = false;

	public static String elasticsearchScheme = "http";
	public static String elasticsearchHostname = "localhost";
	public static int elasticsearchPort = 9200;

	public static String elasticsearchIndex = "elastictriples-edp";

	private ElasticsearchQuery elasticsearchQuery;

	@SuppressWarnings("resource")
	@Before
	public void setUp() throws Exception {

		// Test has to be enabled
		Assume.assumeTrue(EXECUTE_TESTS);

		// Ping Elasticsearch
		try {
			elasticsearchQuery = new ElasticsearchQuery()
					.setHttpHost(new HttpHost(elasticsearchHostname, elasticsearchPort, elasticsearchScheme));
			Assume.assumeTrue(elasticsearchQuery.ping());
		} catch (Exception e) {
			System.err.println("Could not connect: " + e.getMessage());
			assumeTrue(false);
		}

		// Configure
		elasticsearchQuery.setIndex(elasticsearchIndex);
	}

	@After
	public void tearDown() throws Exception {
		if (elasticsearchQuery != null) {
			elasticsearchQuery.close();
		}
	}

	@Test
	public void testQueryDatasets() throws Exception {
		Assume.assumeTrue(EXECUTE_QUERY_DATASETS);
		List<String> datasets = elasticsearchQuery.getAllDatasets();
		Assert.assertFalse(datasets.isEmpty());
		System.out.println(datasets.get(1));
	}

	@Test
	public void testGetDataset() throws Exception {
		Assume.assumeTrue(EXECUTE_QUERY_DATASET_GRAPH);
		long time = System.currentTimeMillis();
		String dataset = "http://projekt-opal.de/dataset/_mcloudde_baysisstraennetz";
		dataset = "http://projekt-opal.de/dataset/https___ckan_govdata_de_1e19b5f3_5258_558c_8744_400e1727cab9";
		StringBuilder nTripleLines = new StringBuilder();
		List<String> datasetRequestUris = new LinkedList<>();
		datasetRequestUris.add(dataset);
		int calls = elasticsearchQuery.getDatasetGraph(datasetRequestUris, nTripleLines);
		System.out.println(nTripleLines);
		System.out.println("Request time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");

		System.out.println("Calls: " + calls);

		Model model = new Serialization().deserialize(nTripleLines.toString());
		System.out.println(model);
		System.out.println("Triples in model: " + model.size());

		System.out.println("Overall time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}

}
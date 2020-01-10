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
	public static final boolean EXECUTE_FILTER_DATASETS = false;

	public static String elasticsearchScheme = "http";
	public static String elasticsearchHostname = "localhost";
	public static int elasticsearchPort = 9200;

	public static String elasticsearchIndex = "elastictriples-edp-deen";

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

	@SuppressWarnings("deprecation")
	@Test
	public void testGetDataset() throws Exception {
		Assume.assumeTrue(EXECUTE_QUERY_DATASET_GRAPH);

		String dataset = "https://europeandataportal.eu/set/data/cz-00025712-cuzk_bu_549525";
		dataset = "https://europeandataportal.eu/set/data/9a650b4f-c7b5-3be7-8e0f-36e77f4393a4";
		dataset = "https://europeandataportal.eu/set/data/7088d912-9a4a-3ff0-9aa8-257e7a48a4e0";
		boolean iterative = true;

		long time = System.currentTimeMillis();
		StringBuilder nTripleLines = new StringBuilder();
		List<String> datasetRequestUris = new LinkedList<>();
		datasetRequestUris.add(dataset);
		float calls;
		if (iterative) {
			calls = elasticsearchQuery.getDatasetGraphIterative(datasetRequestUris.get(0), nTripleLines);
			// Triples in model: 235
			// Calls: 5066.003
			// Request time: 8.726 seconds
			// Overall time: 9.375 seconds

		} else {
			calls = elasticsearchQuery.getDatasetGraphRecursive(datasetRequestUris, nTripleLines);
			// Triples in model: 235
			// Calls: 5066.004
			// Request time: 9.0 seconds
			// Overall time: 9.733 seconds
		}
		System.out.println(nTripleLines);
		float timeRequest = (System.currentTimeMillis() - time) / 1000f;

		System.out.println();

		Model model = new Serialization().deserialize(nTripleLines.toString());
		System.out.println(model);

		System.out.println();
		System.out.println("Triples in model: " + model.size());
		System.out.println("Calls: " + calls);
		System.out.println("Request time: " + timeRequest + " seconds");
		System.out.println("Overall time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}

	@Test
	public void testFilterDatasets() throws Exception {
		Assume.assumeTrue(EXECUTE_FILTER_DATASETS);

		String dataset = "https://europeandataportal.eu/set/data/7088d912-9a4a-3ff0-9aa8-257e7a48a4e0";

		Assert.assertTrue(elasticsearchQuery.isDatasetInLanguage(dataset, "de"));
		Assert.assertFalse(elasticsearchQuery.isDatasetInLanguage(dataset, "en"));
	}
}
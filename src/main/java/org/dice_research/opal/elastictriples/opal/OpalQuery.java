package org.dice_research.opal.elastictriples.opal;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.dice_research.opal.elastictriples.ElasticsearchQuery;
import org.dice_research.opal.elastictriples.Serialization;

/**
 * Wrapper for OPAL queries.
 *
 * @author Adrian Wilke
 */
public class OpalQuery {

	public static Model getDatasetGraph(ElasticsearchQuery elasticsearchQuery, List<String> datasetUris)
			throws IOException {
		StringBuilder nTripleLines = new StringBuilder();
		elasticsearchQuery.getDatasetGraph(datasetUris, nTripleLines);
		return new Serialization().deserialize(nTripleLines.toString());
	}

	/**
	 * Examples.
	 */
	public static void main(String[] args) throws Exception {

		// Configuration
		OpalConfig.elasticsearchScheme = "http";
		OpalConfig.elasticsearchHostname = "localhost";
		OpalConfig.elasticsearchPort = 9200;

		OpalConfig.elasticsearchIndex = "elastictriples-test";

		// Try to connect
		ElasticsearchQuery elasticsearchQuery = null;
		try {
			elasticsearchQuery = OpalConfig.get();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Get all dataset URIs
		List<String> datasetUris = elasticsearchQuery.getAllDatasets();
		System.out.println("Number of datasets: " + datasetUris.size());

		// Get a dataset graph
		int numberOfDatasetsToRequest = 10;
		List<String> datasetRequestUris = new LinkedList<>();
		for (int i = 0; i < Math.min(numberOfDatasetsToRequest, datasetUris.size()); i++) {
			System.out.println("Will request: " + datasetUris.get(i));
			datasetRequestUris.add(datasetUris.get(i));
		}
		Model datasetGraph = OpalQuery.getDatasetGraph(OpalConfig.get(), datasetRequestUris);
		System.out.println("Resulting graph size: " + datasetGraph.size());

		elasticsearchQuery.close();
	}
}
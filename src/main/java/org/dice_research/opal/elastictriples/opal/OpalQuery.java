package org.dice_research.opal.elastictriples.opal;

import java.io.IOException;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.dice_research.opal.elastictriples.ElasticsearchQuery;
import org.dice_research.opal.elastictriples.Serialization;

/**
 * Wrapper for OPAL queries (Getting DCAT dataset graphs).
 *
 * @author Adrian Wilke
 */
public class OpalQuery {

	/**
	 * Example.
	 */
	public static void main(String[] args) throws Exception {

		// Elasticsearch service
		OpalConfig.elasticsearchScheme = "http";
		OpalConfig.elasticsearchHostname = "localhost";
		OpalConfig.elasticsearchPort = 9200;

		// Elasticsearch index
		OpalConfig.elasticsearchIndex = "elastictriples-edp";

		// Get all dataset URIs
		List<String> datasetUris = null;
		try (ElasticsearchQuery elasticsearchQuery = OpalConfig.get()) {
			datasetUris = elasticsearchQuery.getAllDatasets();
			System.out.println("Number of datasets: " + datasetUris.size());
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Get a dataset graph
		try (ElasticsearchQuery elasticsearchQuery = OpalConfig.reset()) {
			Model datasetGraph = OpalQuery.getDatasetGraph(OpalConfig.get(), datasetUris.get(0));
			System.out.println("Resulting graph size: " + datasetGraph.size());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static Model getDatasetGraph(ElasticsearchQuery elasticsearchQuery, String datasetUri) throws IOException {
		StringBuilder nTripleLines = new StringBuilder();
		elasticsearchQuery.getDatasetGraphIterative(datasetUri, nTripleLines);
		return new Serialization().deserialize(nTripleLines.toString());
	}
}
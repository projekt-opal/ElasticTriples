package org.dice_research.opal.elastictriples.opal;

import java.io.IOException;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.dice_research.opal.elastictriples.ElasticsearchQuery;
import org.dice_research.opal.elastictriples.Serialization;

public class OpalQuery {

	public static Model getDatasetGraph(ElasticsearchQuery elasticsearchQuery, String datasetUri) throws IOException {
		StringBuilder nTripleLines = new StringBuilder();
		elasticsearchQuery.getDatasetGraph(datasetUri, nTripleLines);
		return new Serialization().deserialize(nTripleLines.toString());
	}

	/**
	 * Examples
	 */
	public static void main(String[] args) throws Exception {

		// Configuration
		OpalConfig.elasticsearchScheme = "http";
		OpalConfig.elasticsearchHostname = "localhost";
		OpalConfig.elasticsearchPort = 9200;
		OpalConfig.elasticsearchIndex = "elastictriples-edp";

		ElasticsearchQuery elasticsearchQuery = OpalConfig.get();

		// Get all dataset URIs
		List<String> datasetUris = elasticsearchQuery.getAllDatasets();
		System.out.println(datasetUris.size());

		// Get a dataset graph
		String datasetUri = "http://projekt-opal.de/dataset/https___ckan_govdata_de_1e19b5f3_5258_558c_8744_400e1727cab9";
		Model datasetGraph = OpalQuery.getDatasetGraph(OpalConfig.get(), datasetUri);
		System.out.println(datasetGraph.size());

		elasticsearchQuery.close();
	}
}
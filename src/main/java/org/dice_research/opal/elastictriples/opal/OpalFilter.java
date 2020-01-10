package org.dice_research.opal.elastictriples.opal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.dice_research.opal.elastictriples.ElasticsearchQuery;

/**
 * Filters by language tags of titles.
 *
 * @author Adrian Wilke
 */
public class OpalFilter {

	/**
	 * Example.
	 */
	public static void main(String[] args) throws Exception {

		// Elasticsearch service
		OpalConfig.elasticsearchScheme = "http";
		OpalConfig.elasticsearchHostname = "localhost";
		OpalConfig.elasticsearchPort = 9200;

		// Elasticsearch index
		OpalConfig.elasticsearchIndex = "elastictriples-edp-deen";

		// Language tag to filter
		String languageTag = "de";

		// Files
		File directoryTmp = new File(System.getProperty("java.io.tmpdir"));
		File directoryOut = new File(directoryTmp, OpalFilter.class.getName());
		directoryOut.mkdirs();
		File fileDatasets = new File(directoryOut, "datasetUris.txt");
		File fileFiltered = new File(directoryOut, "datasetUrisFiltered.txt");

		// Get all dataset URIs
		List<String> datasetUris = null;
		if (fileDatasets.canRead()) {
			datasetUris = FileUtils.readLines(fileDatasets, Charset.defaultCharset());
		} else {
			try (ElasticsearchQuery elasticsearchQuery = OpalConfig.get()) {
				datasetUris = elasticsearchQuery.getAllDatasets();
			} catch (Exception e) {
				e.printStackTrace();
			}
			FileUtils.writeLines(fileDatasets, datasetUris);
		}
		System.out.println("Number of datasets: " + datasetUris.size());
		System.out.println("File: " + fileDatasets.getAbsolutePath());

		// Filter by language tag
		List<String> datasetUrisFiltered = new LinkedList<>();
		if (fileFiltered.canRead()) {
			datasetUrisFiltered = FileUtils.readLines(fileFiltered, Charset.defaultCharset());
		} else {
			for (int i = 0; i < datasetUris.size(); i += 10000) {
				List<String> sublist = datasetUris.subList(i, Math.min(i + 10000, datasetUris.size()));
				try (ElasticsearchQuery elasticsearchQuery = OpalConfig.reset()) {
					datasetUrisFiltered.addAll(elasticsearchQuery.filterByLanguage(sublist, languageTag));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			FileUtils.writeLines(fileFiltered, datasetUrisFiltered);
		}
		System.out.println("Number of filtered datasets: " + datasetUrisFiltered.size());
		System.out.println("File: " + fileFiltered.getAbsolutePath());

		// Get a dataset graph
		int counter = 0;
		for (int i = 0; i < datasetUrisFiltered.size(); i += 5000) {
			File file = new File(directoryOut, "data." + ++counter + ".nt");
			BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
			try (ElasticsearchQuery elasticsearchQuery = OpalConfig.reset()) {
				for (int j = i; j < Math.min(i + 5000, datasetUrisFiltered.size()); j++) {
					writer.write(OpalFilter.getDatasetGraph(OpalConfig.get(), datasetUrisFiltered.get(j)));
					writer.newLine();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			writer.close();
		}
	}

	public static String getDatasetGraph(ElasticsearchQuery elasticsearchQuery, String datasetUri) throws IOException {
		StringBuilder nTripleLines = new StringBuilder();
		elasticsearchQuery.getDatasetGraphIterative(datasetUri, nTripleLines);
		return nTripleLines.toString();
	}
}
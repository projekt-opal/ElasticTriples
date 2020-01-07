package org.dice_research.opal.elastictriples.opal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.dice_research.opal.elastictriples.ElasticsearchQuery;

/**
 * Splits data of DCAT dataset graphs into several files.
 *
 * @author Adrian Wilke
 */
public class OpalSplitter {

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

		// Size of output files
		int datasetsPerFile = 10 * 1000;

		// Directory for files to create
		String outputDirectory = "/tmp/elastictriples/split";

		// Split data
		run(datasetsPerFile, outputDirectory);
	}

	public static void run(int datasetsPerFile, String outputDirectory) throws Exception {

		long time = System.currentTimeMillis();

		// Check output directory
		File directory = new File(outputDirectory);
		if (!directory.exists()) {
			directory.mkdirs();
		} else {
			if (!directory.isDirectory()) {
				System.err.println("Not a directory: " + directory.getAbsolutePath());
				System.exit(1);
			}
			if (!directory.canWrite()) {
				System.err.println("Can not write to directory: " + directory.getAbsolutePath());
				System.exit(1);
			}
		}

		// Get all dataset URIs
		List<String> datasetUris = null;
		try (ElasticsearchQuery elasticsearchQuery = OpalConfig.get()) {
			System.out.println("Requesting dataset URIs");
			datasetUris = elasticsearchQuery.getAllDatasets();
			System.out.println("Got dataset URIs: " + datasetUris.size());
		}

		// Split
		int counterDatasets = 0;
		int counterFiles = 1;
		System.out.println("Writing to " + directory);
		File file = new File(directory, "data." + counterFiles + ".nt");
		BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
		OpalConfig.reset();
		for (String datasetUri : datasetUris) {

			// Update iteration
			counterDatasets++;
			if (counterDatasets >= datasetsPerFile) {

				// Update counters
				counterDatasets = 0;
				counterFiles++;

				// Update file writer
				writer.close();
				file = new File(directory, "data." + counterFiles + ".nt");
				writer = new BufferedWriter(new FileWriter(file, true));
				System.out.println(file.getAbsolutePath());

				// Update Elasticsearch connection
				OpalConfig.elasticsearchQuery.close();
				OpalConfig.reset();
			}

			try {
				writer.write(OpalSplitter.getDatasetGraph(OpalConfig.get(), datasetUri));
				writer.newLine();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		// Finally close resources
		OpalConfig.elasticsearchQuery.close();
		writer.close();

		System.out.println("Time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}

	public static String getDatasetGraph(ElasticsearchQuery elasticsearchQuery, String datasetUri) throws IOException {
		StringBuilder nTripleLines = new StringBuilder();
		elasticsearchQuery.getDatasetGraphIterative(datasetUri, nTripleLines);
		return nTripleLines.toString();
	}
}
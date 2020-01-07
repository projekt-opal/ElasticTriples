package org.dice_research.opal.elastictriples.opal;

import java.io.File;

import org.apache.http.HttpHost;
import org.dice_research.opal.elastictriples.ElasticsearchImporter;

/**
 * Imports OPAL source portal data.
 *
 * @author Adrian Wilke
 */
public class OpalImport {

	/**
	 * Example.
	 */
	public static void main(String[] args) throws Exception {

		// Data to import
		File file = new File("/tmp/opal-rdf/edp.nt");

		// Data format
		String language = "N-Triples";

		// Elasticsearch service
		OpalConfig.elasticsearchScheme = "http";
		OpalConfig.elasticsearchHostname = "localhost";
		OpalConfig.elasticsearchPort = 9200;

		// Elasticsearch index
		OpalConfig.elasticsearchIndex = "elastictriples-edp";

		// Import data
		new OpalImport().opalImport(file, language);
	}

	public void opalImport(File file, String language) throws Exception {

		long time = System.currentTimeMillis();

		try (ElasticsearchImporter elasticsearchImporter = new ElasticsearchImporter()) {

			elasticsearchImporter
					.setHttpHost(new HttpHost(OpalConfig.elasticsearchHostname, OpalConfig.elasticsearchPort,
							OpalConfig.elasticsearchScheme))

					.setIndex(OpalConfig.elasticsearchIndex);

			try {
				if (!elasticsearchImporter.ping()) {
					System.err.println("Could not ping Elasticsearch");
					System.exit(1);
				}
			} catch (Exception e) {
				System.err.println("Could not connect: " + e.getMessage());
				System.exit(1);
			}

			if (!elasticsearchImporter.indexExists()) {
				elasticsearchImporter.createIndex(true);
			}

			elasticsearchImporter.importFile(file, language);
		}

		System.out.println("Import time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}
}
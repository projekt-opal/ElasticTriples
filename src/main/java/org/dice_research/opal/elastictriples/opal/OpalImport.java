package org.dice_research.opal.elastictriples.opal;

import java.io.File;

import org.apache.http.HttpHost;
import org.dice_research.opal.elastictriples.ElasticsearchImporter;

public class OpalImport {

	public static File file = new File("/tmp/test.nt");
	public static String language = "N-Triples";

	public void opalImport() throws Exception {

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
				elasticsearchImporter.createIndex();
			}

			elasticsearchImporter.importFile(file, language);
		}

		System.out.println("Import time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}

	/**
	 * Example.
	 */
	public static void main(String[] args) throws Exception {
		new OpalImport().opalImport();
	}
}
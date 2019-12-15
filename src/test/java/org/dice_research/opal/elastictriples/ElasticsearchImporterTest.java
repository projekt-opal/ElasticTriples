package org.dice_research.opal.elastictriples;

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.net.URISyntaxException;

import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link ElasticsearchImporter}.
 * 
 * @see http://localhost:9200/_cat/indices?v to check indices
 * @see http://localhost:9200/{index} to check mappings
 *
 * @author Adrian Wilke
 */
public class ElasticsearchImporterTest {

	// Set to true if you have configured Elasticsearch
	public static final boolean EXECUTE_TESTS = false;
	public static final boolean EXECUTE_SPECIFIC_FILE_IMPORT = false;
	public static final boolean EXECUTE_NTRIPLE_FILE_IMPORT = false;

	public static String elasticsearchScheme = "http";
	public static String elasticsearchHostname = "localhost";
	public static int elasticsearchPort = 9200;

	public static String elasticsearchIndex = "elastictriples3";

	public static File file = new File("/tmp/test.nt");
	public static String language = "N-Triples";

	private ElasticsearchImporter elasticsearchImporter;

	@Before
	public void setUp() throws Exception {

		// Test has to be enabled
		Assume.assumeTrue(EXECUTE_TESTS);

		// Ping Elasticsearch
		try {
			elasticsearchImporter = new ElasticsearchImporter()
					.setHttpHost(new HttpHost(elasticsearchHostname, elasticsearchPort, elasticsearchScheme));
			Assume.assumeTrue(elasticsearchImporter.ping());
		} catch (Exception e) {
			System.err.println("Could not connect: " + e.getMessage());
			assumeTrue(false);
		}
	}

	@After
	public void tearDown() throws Exception {
		if (elasticsearchImporter != null) {
			elasticsearchImporter.close();
		}
	}

	public ElasticsearchImporterTest() throws URISyntaxException {

	}

	@Test
	public void testNtripleFileImport() throws Exception {
		Assume.assumeTrue(EXECUTE_NTRIPLE_FILE_IMPORT);
		importFile(new File(ElasticsearchImporterTest.class.getClassLoader().getResource("triples.nt").toURI()),
				"N-Triples");
	}

	@Test
	public void testSpecificFileImport() throws Exception {
		long time = System.currentTimeMillis();
		Assume.assumeTrue(EXECUTE_SPECIFIC_FILE_IMPORT);
		importFile(file, language);
		System.out.println("Import time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}

	protected void importFile(File file, String language) throws Exception {
		Assume.assumeTrue(file.canRead());

		elasticsearchImporter.setIndex(elasticsearchIndex);

		if (!elasticsearchImporter.indexExists()) {
			elasticsearchImporter.createIndex();
		}

		elasticsearchImporter.importFile(file, language);
	}

}
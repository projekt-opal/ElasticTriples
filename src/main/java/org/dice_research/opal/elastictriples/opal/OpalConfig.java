package org.dice_research.opal.elastictriples.opal;

import org.apache.http.HttpHost;
import org.dice_research.opal.elastictriples.ElasticsearchQuery;

/**
 * Elasticsearch configuration for OPAL.
 *
 * @author Adrian Wilke
 */
public class OpalConfig {

	public static String elasticsearchScheme = "http";
	public static String elasticsearchHostname = "localhost";
	public static int elasticsearchPort = 9200;

	public static String elasticsearchIndex = "elastictriples-test";

	public static ElasticsearchQuery elasticsearchQuery;

	public static ElasticsearchQuery get() throws Exception {
		if (elasticsearchQuery == null) {
			elasticsearchQuery = reset();
		}
		return elasticsearchQuery;
	}

	@SuppressWarnings("resource")
	public static ElasticsearchQuery reset() throws Exception {
		elasticsearchQuery = new ElasticsearchQuery()

				.setHttpHost(new HttpHost(OpalConfig.elasticsearchHostname, OpalConfig.elasticsearchPort,
						OpalConfig.elasticsearchScheme))

				.setIndex(OpalConfig.elasticsearchIndex);

		try {
			if (!elasticsearchQuery.ping()) {
				throw new Exception("Could not ping Elasticsearch");
			}
		} catch (Exception e) {
			throw new Exception("Could not reach Elasticsearch", e);
		}

		return elasticsearchQuery;
	}
}
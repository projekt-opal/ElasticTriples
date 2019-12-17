package org.dice_research.opal.elastictriples.opal;

import org.apache.http.HttpHost;
import org.dice_research.opal.elastictriples.ElasticsearchQuery;

public class OpalConfig {

	public static String elasticsearchScheme = "http";
	public static String elasticsearchHostname = "localhost";
	public static int elasticsearchPort = 9200;
	public static String elasticsearchIndex = "elastictriples-edp";

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

		if (!elasticsearchQuery.ping()) {
			throw new Exception("Could not ping Elasticsearch");
		}

		return elasticsearchQuery;
	}
}
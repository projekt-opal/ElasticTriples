package org.dice_research.opal.elastictriples;

import java.io.Closeable;
import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * Elasticsearch connection.
 * 
 * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
 * @see http://localhost:9200/_cat/indices?v
 *
 * @author Adrian Wilke
 */
public class Elasticsearch implements Closeable {

	protected HttpHost httpHost;
	protected String index;
	protected RestHighLevelClient restHighLevelClient = null;

	public HttpHost getHttpHost() {
		return httpHost;
	}

	public Elasticsearch setHttpHost(HttpHost httpHost) {
		this.httpHost = httpHost;
		return this;
	}

	public String getIndex() {
		return index;
	}

	public Elasticsearch setIndex(String index) {
		this.index = index;
		return this;
	}

	public Elasticsearch createIndex() throws IOException {
		System.out.println(
				"Creating index for " + getIndex() + " at " + getHttpHost() + " " + Elasticsearch.class.getName());

		CreateIndexRequest request = new CreateIndexRequest(getIndex());

		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.startObject("properties");
			{
				builder.startObject("subject");
				{
					builder.field("type", "keyword");
				}
				builder.endObject();
				builder.startObject("predicate");
				{
					builder.field("type", "keyword");
				}
				builder.endObject();
				builder.startObject("object");
				{
					builder.field("type", "text");
				}
				builder.endObject();
			}
			builder.endObject();
		}
		builder.endObject();
		request.mapping(builder);

		try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHost))) {
			client.indices().create(request, RequestOptions.DEFAULT);
		}

		return this;
	}

	public boolean ping() throws IOException {
		try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHost))) {
			return client.ping(RequestOptions.DEFAULT);
		}
	}

	public boolean indexExists() throws IOException {
		GetIndexRequest request = new GetIndexRequest(getIndex());
		try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHost))) {
			return client.indices().exists(request, RequestOptions.DEFAULT);
		}
	}

	/**
	 * Gets Elasticsearch client.
	 */
	public RestHighLevelClient getRestHighLevelClient() throws IOException {
		if (restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHost));
		}
		return restHighLevelClient;
	}

	@Override
	public void close() throws IOException {
		if (restHighLevelClient != null) {
			restHighLevelClient.close();
		}
	}

}
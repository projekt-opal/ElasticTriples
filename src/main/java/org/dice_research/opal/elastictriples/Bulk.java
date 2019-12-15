package org.dice_research.opal.elastictriples;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;

/**
 * Executes multiple requests.
 * 
 * Initialize with {@link #setElasticsearch(Elasticsearch)}.
 * 
 * (Optional) Configure with {@link #setNumberOfRequests(int)}.
 * 
 * Add requests with {@link #add(DocWriteRequest)} or {@link #add(Iterable)}. If
 * return value is true, call {@link #getResponse()}.
 * 
 * Finally call {@link #finalRequest()}. If return value is true, call
 * {@link #getResponse()}.
 * 
 * @author Adrian Wilke
 */
public class Bulk {

	protected Elasticsearch elasticsearch;
	protected int numberOfRequests = 10000;

	protected BulkRequest bulkRequest = new BulkRequest();
	protected BulkResponse bulkResponse = null;

	public Bulk setElasticsearch(Elasticsearch elasticsearch) {
		this.elasticsearch = elasticsearch;
		return this;
	}

	public Bulk setNumberOfRequests(int numberOfRequests) {
		this.numberOfRequests = numberOfRequests;
		return this;
	}

	public int getNumberOfRequests() {
		return numberOfRequests;
	}

	/**
	 * Adds {@link DeleteRequest}, {@link IndexRequest}, or {@link UpdateRequest}.
	 * 
	 * Returns, if client was requested.
	 */
	public boolean add(DocWriteRequest<?> docWriteRequest) throws Exception {
		if (isResponseAvailable()) {
			throw new Exception("Trying to add request while response is available.");
		} else {
			bulkRequest.add(docWriteRequest);
			return conditionalRequest();
		}
	}

	/**
	 * Adds {@link Iterable} containing {@link DeleteRequest}, {@link IndexRequest},
	 * or {@link UpdateRequest}.
	 * 
	 * Returns, if client was requested.
	 */
	public boolean add(Iterable<DocWriteRequest<?>> requests) throws Exception {
		if (isResponseAvailable()) {
			throw new Exception("Trying to add requests while response is available.");
		} else {
			bulkRequest.add(requests);
			return conditionalRequest();
		}
	}

	/**
	 * Requests client on open requests.
	 * 
	 * Returns, if open requests existed and new response was created.
	 */
	public boolean finalRequest() throws Exception {
		if (isResponseAvailable()) {
			throw new Exception("Trying to request while response is available.");
		} else if (bulkRequest.numberOfActions() != 0) {
			this.bulkResponse = request();
			return true;
		} else {
			return false;
		}

	}

	public boolean isResponseAvailable() {
		return bulkResponse != null;
	}

	private boolean conditionalRequest() throws Exception {
		if (bulkRequest.numberOfActions() >= numberOfRequests) {
			this.bulkResponse = request();
			return true;
		} else {
			return false;
		}
	}

	private BulkResponse request() throws Exception {
		if (elasticsearch == null || elasticsearch.httpHost == null) {
			throw new Exception("No Elasticsearch host set.");
		}
		if (bulkRequest != null) {
			return elasticsearch.getRestHighLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
		} else {
			return null;
		}
	}

	public BulkResponse getBulkResponse() throws Exception {
		BulkResponse bulkResponse = this.bulkResponse;
		this.bulkResponse = null;
		this.bulkRequest = new BulkRequest();
		return bulkResponse;
	}

}
package org.dice_research.opal.elastictriples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.jena.vocabulary.DCAT;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Queries data from Elasticsearch
 *
 * @author Adrian Wilke
 */
public class ElasticsearchQuery extends Elasticsearch {

	public float getDatasetGraphIterative(String datasetUri, StringBuilder nTripleLines) throws IOException {
		// TODO Temporary recursive implementation for dataset graph
		float counterRequests = 0f;
		int counterMulti = 0;

		List<SearchRequest> searchRequests = new LinkedList<>();

		// Get source catalog(s)
		if (nTripleLines.length() == 0) {
			MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("object", datasetUri);

			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(matchPhraseQueryBuilder);
			sourceBuilder.size(10000);

			SearchRequest searchRequest = new SearchRequest(getIndex());
			searchRequest.source(sourceBuilder);
			searchRequests.add(searchRequest);
		}

		List<String> requestedResources = new LinkedList<>();
		List<String> newResources = new LinkedList<>();

		newResources.add(datasetUri);
		while (!newResources.isEmpty()) {

			// Create new requests
			for (String newResource : newResources) {
				searchRequests.add(createSearchRequest(newResource));
				requestedResources.add(newResource);
			}
			newResources.clear();

			// Request
			for (SearchHit hit : multiSearchQuery(searchRequests)) {
				Triple triple = new Triple();
				triple.subject = (String) hit.getSourceAsMap().get("subject");
				triple.predicate = (String) hit.getSourceAsMap().get("predicate");
				triple.object = (String) hit.getSourceAsMap().get("object");

				// Add results
				nTripleLines.append(triple.getNtriples());
				nTripleLines.append(System.lineSeparator());

				// Prepare next iteration
				if (!requestedResources.contains(triple.object)) {
					if (!Triple.isLiteral(triple.object)) {
						newResources.add(triple.object);
					}
				}
			}
			counterRequests += searchRequests.size();
			counterMulti++;
			searchRequests.clear();
		}

		return counterRequests + (1f * counterMulti / 1000);
	}

	@Deprecated
	/**
	 * Use {@link #getDatasetGraphIterative(String, StringBuilder)} instead.
	 */
	public float getDatasetGraphRecursive(List<String> datasetUris, StringBuilder nTripleLines) throws IOException {
		// TODO Temporary recursive implementation for dataset graph
		float counterRequests = 0f;
		int counterMulti = 0;

		// Get source catalog(s)
		if (nTripleLines.length() == 0) {
			List<SearchRequest> searchRequests = new LinkedList<>();
			for (String datasetUri : datasetUris) {
				MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("object", datasetUri);

				SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
				sourceBuilder.query(matchPhraseQueryBuilder);
				sourceBuilder.size(10000);

				SearchRequest searchRequest = new SearchRequest(getIndex());
				searchRequest.source(sourceBuilder);
				searchRequests.add(searchRequest);
			}

			counterRequests += searchRequests.size();
			counterMulti++;
			for (SearchHit hit : multiSearchQuery(searchRequests)) {
				Triple triple = new Triple();
				triple.subject = (String) hit.getSourceAsMap().get("subject");
				triple.predicate = (String) hit.getSourceAsMap().get("predicate");
				triple.object = (String) hit.getSourceAsMap().get("object");
				nTripleLines.append(triple.getNtriples());
				nTripleLines.append(System.lineSeparator());
			}
		}

		// Create search requests
		List<SearchRequest> searchRequests = new LinkedList<>();
		for (String datasetUri : datasetUris) {
			searchRequests.add(createSearchRequest(datasetUri));
		}

		// Extract data
		counterRequests += searchRequests.size();
		counterMulti++;
		List<String> objects = new LinkedList<>();
		for (SearchHit hit : multiSearchQuery(searchRequests)) {
			Triple triple = new Triple();
			triple.subject = (String) hit.getSourceAsMap().get("subject");
			triple.predicate = (String) hit.getSourceAsMap().get("predicate");
			triple.object = (String) hit.getSourceAsMap().get("object");
			nTripleLines.append(triple.getNtriples());
			nTripleLines.append(System.lineSeparator());

			if (!Triple.isLiteral(triple.object)) {
				objects.add(triple.object);
			}
		}

		// Recursively create graph
		if (!objects.isEmpty()) {
			counterRequests += getDatasetGraphRecursive(objects, nTripleLines);
		}

		return counterRequests + (1f * counterMulti / 1000);
	}

	public List<String> getAllDatasets() throws IOException {
		// TODO Temporary implementation for dcat:Dataset
		String object = DCAT.Dataset.toString();
		MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("object", object);
		List<SearchHit> hits = scrollQuery(matchPhraseQueryBuilder);
		List<String> datasets = new ArrayList<>(hits.size());
		for (SearchHit searchHit : hits) {
			if (searchHit.getSourceAsMap().containsKey("subject")) {
				datasets.add((String) searchHit.getSourceAsMap().get("subject"));
			}
		}
		return datasets;
	}

	public SearchRequest createSearchRequest(String subject) {
		// TODO Temporary implementation for subject search
		MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("subject", subject);

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(matchPhraseQueryBuilder);
		sourceBuilder.size(10000);

		SearchRequest searchRequest = new SearchRequest(getIndex());
		searchRequest.source(sourceBuilder);

		return searchRequest;
	}

	public List<SearchHit> multiSearchQuery(List<SearchRequest> searchRequests) throws IOException {
		MultiSearchRequest request = new MultiSearchRequest();
		for (SearchRequest searchRequest : searchRequests) {
			request.add(searchRequest);
		}

		MultiSearchResponse response = getRestHighLevelClient().msearch(request, RequestOptions.DEFAULT);

		List<SearchHit> searchHits = new LinkedList<>();
		for (Item item : response.getResponses()) {
			if (item.getFailure() != null) {
				System.err.println("Failure: " + item.getFailureMessage() + " " + getClass().getName());
			}
			searchHits.addAll(Arrays.asList(item.getResponse().getHits().getHits()));
		}
		return searchHits;
	}

	public List<SearchHit> scrollQuery(QueryBuilder queryBuilder) throws IOException {

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(queryBuilder);
		sourceBuilder.size(10000);

		SearchRequest searchRequest = new SearchRequest(getIndex());
		searchRequest.source(sourceBuilder);
		searchRequest.scroll(TimeValue.timeValueMinutes(1L));

		// First request
		int numberOfHits = 0;
		SearchResponse searchResponse = getRestHighLevelClient().search(searchRequest, RequestOptions.DEFAULT);
		String scrollId = searchResponse.getScrollId();
		SearchHits searchHits = searchResponse.getHits();
		int initialNumber = (int) Math.min(Integer.MAX_VALUE, searchHits.getTotalHits().value);
		List<SearchHit> list = new ArrayList<>(initialNumber);
		list.addAll(Arrays.asList(searchHits.getHits()));
		numberOfHits = searchHits.getHits().length;

		// Scroll
		while (numberOfHits != 0) {
			SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
			scrollRequest.scroll(TimeValue.timeValueSeconds(30));
			SearchResponse searchScrollResponse = getRestHighLevelClient().scroll(scrollRequest,
					RequestOptions.DEFAULT);
			scrollId = searchScrollResponse.getScrollId();
			list.addAll(Arrays.asList(searchScrollResponse.getHits().getHits()));
			numberOfHits = searchScrollResponse.getHits().getHits().length;
		}

		// Clear
		ClearScrollRequest request = new ClearScrollRequest();
		request.addScrollId(scrollId);
		getRestHighLevelClient().clearScroll(request, RequestOptions.DEFAULT);

		// Check
		if (searchHits.getTotalHits().value != list.size()) {
			System.err.println("Unexpected number of hits: " + list.size() + " insted of "
					+ searchHits.getTotalHits().value + " " + getClass().getName());
		}

		return list;
	}

	@Override
	public ElasticsearchQuery setHttpHost(HttpHost httpHost) {
		super.setHttpHost(httpHost);
		return this;
	}

	@Override
	public ElasticsearchQuery setIndex(String index) {
		super.setIndex(index);
		return this;
	}

	@Override
	public ElasticsearchQuery createIndex(boolean objectsAsText) throws IOException {
		super.createIndex(objectsAsText);
		return this;
	}

}
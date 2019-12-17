package org.dice_research.opal.elastictriples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.jena.vocabulary.DCAT;
import org.elasticsearch.action.search.ClearScrollRequest;
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

	public int getDatasetGraph(String datasetUri, StringBuilder nTripleLines) throws IOException {
		// TODO Temporary recursive implementation for dataset graph
		int calls = 1;

		if (nTripleLines.length() == 0) {
			MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("object", datasetUri);

			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(matchPhraseQueryBuilder);
			sourceBuilder.size(10000);

			SearchRequest searchRequest = new SearchRequest(getIndex());
			searchRequest.source(sourceBuilder);

			SearchResponse searchResponse = getRestHighLevelClient().search(searchRequest, RequestOptions.DEFAULT);

			for (SearchHit hit : searchResponse.getHits()) {
				Triple triple = new Triple();
				triple.subject = (String) hit.getSourceAsMap().get("subject");
				triple.predicate = (String) hit.getSourceAsMap().get("predicate");
				triple.object = (String) hit.getSourceAsMap().get("object");
				nTripleLines.append(triple.getNtriples());
				nTripleLines.append(System.lineSeparator());
			}
		}

		MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("subject", datasetUri);

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(matchPhraseQueryBuilder);
		sourceBuilder.size(10000);

		SearchRequest searchRequest = new SearchRequest(getIndex());
		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = getRestHighLevelClient().search(searchRequest, RequestOptions.DEFAULT);

		for (SearchHit hit : searchResponse.getHits()) {
			Triple triple = new Triple();
			triple.subject = (String) hit.getSourceAsMap().get("subject");
			triple.predicate = (String) hit.getSourceAsMap().get("predicate");
			triple.object = (String) hit.getSourceAsMap().get("object");
			nTripleLines.append(triple.getNtriples());
			nTripleLines.append(System.lineSeparator());

			calls += getDatasetGraph(triple.object, nTripleLines);
		}

		return calls;
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
	public ElasticsearchQuery createIndex() throws IOException {
		super.createIndex();
		return this;
	}

}
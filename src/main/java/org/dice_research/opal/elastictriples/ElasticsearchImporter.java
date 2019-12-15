package org.dice_research.opal.elastictriples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

/**
 * Imports data to Elasticsearch.
 * 
 * Using a file in N-Triples format is recommended. Other data has to be
 * converted first.
 *
 * @author Adrian Wilke
 */
public class ElasticsearchImporter extends Elasticsearch {

	public ElasticsearchImporter importFile(File file, String language) throws Exception {
		return importFile(file, RDFLanguages.nameToLang(language));

	}

	public ElasticsearchImporter importFile(File file, Lang lang) throws Exception {
		System.out.println(
				"Importing " + lang + " file " + file.getAbsolutePath() + " " + ElasticsearchImporter.class.getName());

		long counter = 0;
		if (lang.equals(Lang.NTRIPLES)) {
			System.out.println(
					"Lines: " + Utils.countLines(file.getAbsolutePath()) + " " + ElasticsearchImporter.class.getName());

			Bulk bulk = new Bulk().setElasticsearch(this);
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				for (String line; (line = br.readLine()) != null;) {
					if (line.isBlank()) {
						continue;
					} else {
						try {
							Triple triple = new Serialization().parseNtriplesLine(line);
							if (bulk.add(createRequest(triple))) {
								counter += handleResponse(bulk.getBulkResponse());
								System.out.println(counter + " " + this.getClass().getName());
							}
						} catch (Exception e) {
							System.err.println(
									"Exception: " + e.getMessage() + " " + line + " " + this.getClass().getName());
						}
					}
				}
			}
			if (bulk.finalRequest()) {
				counter += handleResponse(bulk.getBulkResponse());
			}
			System.out.println(counter);
			return this;

		} else {
			return importUri(file.toURI().toString(), lang);
		}
	}

	public ElasticsearchImporter importUri(String uri, String language) throws Exception {
		return importUri(uri, RDFLanguages.nameToLang(language));
	}

	public ElasticsearchImporter importUri(String uri, Lang lang) throws Exception {
		return importModel(RDFDataMgr.loadModel(uri, lang));
	}

	public ElasticsearchImporter importModel(Model model) throws Exception {
		Bulk bulk = new Bulk().setElasticsearch(this);
		long counter = 0;
		for (Triple triple : new Serialization().serialize(model)) {
			if (bulk.add(createRequest(triple))) {
				counter += handleResponse(bulk.getBulkResponse());
				System.out.println(counter + " " + this.getClass().getName());
			}
		}
		if (bulk.finalRequest()) {
			counter += handleResponse(bulk.getBulkResponse());
		}
		System.out.println(counter);
		return this;
	}

	public ElasticsearchImporter importTriple(Triple triple) throws Exception {
		Bulk bulk = new Bulk().setElasticsearch(this);
		long counter = 0;
		if (bulk.add(createRequest(triple))) {
			counter += handleResponse(bulk.getBulkResponse());
		}
		if (bulk.finalRequest()) {
			counter += handleResponse(bulk.getBulkResponse());
		}
		System.out.println(counter);
		return this;
	}

	protected IndexRequest createRequest(Triple triple) {
		return new IndexRequest(index).source("subject", triple.subject, "predicate", triple.predicate, "object",
				triple.object);
	}

	protected int handleResponse(BulkResponse bulkResponse) {
		int counter = 0;
		for (BulkItemResponse response : bulkResponse.getItems()) {
			if (response.isFailed()) {
				System.err.println("Failed: " + response.getFailureMessage() + " " + this.getClass().getName());
			} else {
				counter++;
			}
		}
		return counter;
	}

	@Override
	public ElasticsearchImporter setHttpHost(HttpHost httpHost) {
		super.setHttpHost(httpHost);
		return this;
	}

	@Override
	public ElasticsearchImporter setIndex(String index) {
		super.setIndex(index);
		return this;
	}

	@Override
	public ElasticsearchImporter createIndex() throws IOException {
		super.createIndex();
		return this;
	}

}
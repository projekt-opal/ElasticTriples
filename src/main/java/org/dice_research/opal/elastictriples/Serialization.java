package org.dice_research.opal.elastictriples;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;

/**
 * Internal data serialization using N-Triples format.
 * 
 * Not thread-save.
 *
 * @author Adrian Wilke
 */
public class Serialization {

	// https://en.wikipedia.org/wiki/N-Triples
	// https://en.wikipedia.org/wiki/Notation3#Comparison_of_Notation3,_Turtle,_and_N-Triples

	// Only one instance required

	protected static final String STR_LANG = RDFLanguages.strLangNTriples;
	protected static final Pattern PREDICATE_PATTERN = Pattern.compile(" <.+?> ");

	// Required for each object, re-used in method calls

	protected StringWriter stringWriter;
	protected Matcher matcher;
	protected int indexPredicateStart, indexPredicateEnd;
	protected Triple triple;

	protected StringReader stringReader;

	/**
	 * Transforms N-Triple lines to model.
	 */
	public Model deserialize(String nTripleLines) {
		stringReader = new StringReader(nTripleLines);
		Model model = ModelFactory.createDefaultModel();
		model.read(stringReader, "", STR_LANG);
		return model;
	}

	/**
	 * Transforms model to triples for import.
	 */
	public List<Triple> serialize(Model model) throws Exception {
		List<Triple> triples = new LinkedList<>();

		// Serialize model
		stringWriter = new StringWriter();
		model.write(stringWriter, STR_LANG);

		// Parse lines
		for (String line : stringWriter.toString().split("\\r?\\n")) {
			if (line.isBlank()) {
				continue;
			} else {
				try {
					triples.add(parseNtriplesLine(line));
				} catch (Exception e) {
					System.err.println("Exception: " + e.getMessage() + " " + line + " " + this.getClass().getName());
				}
			}
		}

		return triples;
	}

	protected Triple parseNtriplesLine(String line) throws Exception {

		// N-Triples serialization cases:
		//
		// _:B6f2...8537 <http://exampe.com/p#b> "literal"@en .
		// <http://exampe.com/s> <http://exampe.com/p#a> _:B6f2...8537 .
		//
		// -> If first character is ignored, the predicate can be identified by
		// brackets. Prefix-point has also to be removed.

		// Find predicate
		matcher = PREDICATE_PATTERN.matcher(line.substring(1));
		if (matcher.find()) {
			indexPredicateStart = matcher.start() + 1;
			indexPredicateEnd = matcher.end() + 1;
		} else {
			throw new Exception("Could not find predicate: " + line);
		}

		Triple triple = new Triple();
		try {
			triple.subject = removeBrackets(line.substring(0, indexPredicateStart));
			triple.predicate = removeBrackets(line.substring(indexPredicateStart + 1, indexPredicateEnd - 1));
			triple.object = removeBrackets(line.substring(indexPredicateEnd, line.length() - 2));
		} catch (Exception e) {
			throw new Exception(line, e);
		}

		return triple;
	}

	protected String removeBrackets(String string) {
		if (string.startsWith("<")) {
			return string.substring(1, string.length() - 1);
		} else {
			return string;
		}
	}
}
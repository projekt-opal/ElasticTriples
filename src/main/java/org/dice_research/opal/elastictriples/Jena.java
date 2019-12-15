package org.dice_research.opal.elastictriples;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParserRegistry;

/**
 * Methods only related to Apache Jena.
 *
 * @author Adrian Wilke
 */
public abstract class Jena {

	public static Set<Lang> getSerializationLanguages() {
		Set<Lang> languages = new HashSet<>();
		for (Lang lang : RDFLanguages.getRegisteredLanguages()) {
			if (RDFParserRegistry.isRegistered(lang)) {
				languages.add(lang);
			}
		}
		return languages;
	}

	public static Set<String> getSerializationLanguageLabels() {
		SortedSet<String> labels = new TreeSet<>();
		for (Lang lang : getSerializationLanguages()) {
			labels.add(lang.getLabel());
		}
		return labels;
	}
}

package org.dice_research.opal.elastictriples;

/**
 * Serialized triple based on N-Triples format.
 * 
 * Blank nodes can be identified by prefix '_:'.
 * 
 * Literals can be identified by prefix '"'.
 *
 * @author Adrian Wilke
 */
public class Triple {

	protected static final String BLANK_NODE_PREFIX = "_:";
	protected static final String LITERAL_PREFIX = "\"";

	protected String subject;
	protected String predicate;
	protected String object;

	public String getNtriplesSubject() {
		if (subject.startsWith(BLANK_NODE_PREFIX)) {
			return subject;
		} else {
			return "<" + subject + ">";
		}
	}

	public String getNtriplesPredicate() {
		return "<" + predicate + ">";
	}

	public String getNtriplesObject() {
		if (object.startsWith(BLANK_NODE_PREFIX) || object.startsWith(LITERAL_PREFIX)) {
			return object;
		} else {
			return "<" + object + ">";
		}
	}

	public String getNtriples() {
		return getNtriplesSubject() + " " + getNtriplesPredicate() + " " + getNtriplesObject() + " .";
	}

	@Override
	public String toString() {
		return getNtriples();
	}
}
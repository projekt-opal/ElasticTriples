package org.dice_research.opal.elastictriples;

import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Tests {@link Serialization}.
 *
 * @author Adrian Wilke
 */
public class SerializationTest {

	// Tests which were used at inital development.
	// The tests do only test Jena.
	public static final boolean EXECUTE_DEV_TESTS = false;
	public static final boolean EXECUTE_BASIC_LANGUAGE_TESTS = false;

	Resource resourceA = ResourceFactory.createResource("http://exampe.com/s#a");
	Resource resourceB = ResourceFactory.createResource("http://exampe.com/s#b");
	Property predicateA = ResourceFactory.createProperty("http://exampe.com/p#a");
	Property predicateB = ResourceFactory.createProperty("http://exampe.com/p#b");
	Property predicateC = ResourceFactory.createProperty("http://exampe.com/p#c");
	Resource blankNode = ResourceFactory.createResource();
	Literal langLiteralA = ResourceFactory.createLangLiteral("literal", "en");
	Literal typedLiteralInteger = ResourceFactory.createTypedLiteral(Integer.valueOf(100));

	protected Model createSimpleModel() {
		Model model = ModelFactory.createDefaultModel();
		model.add(resourceA, predicateA, langLiteralA);
		model.add(resourceA, predicateB, resourceB);
		return model;
	}

	public Model createAdvancedModel() {
		Model model = ModelFactory.createDefaultModel();
		model.add(resourceA, predicateA, blankNode);
		model.add(blankNode, predicateB, langLiteralA);
		model.add(blankNode, predicateC, typedLiteralInteger);
		return model;
	}

	/**
	 * Prints serialization formats.
	 */
	@Test
	public void testExportFormats() {
		Set<String> languages = Jena.getSerializationLanguageLabels();
		Assert.assertTrue(!languages.isEmpty());
		System.out.println(languages);
	}

	/**
	 * Tests serialization of a complete model without blank nodes or literals.
	 */
	@Test
	public void testSimple() throws Exception {
		Serialization serializer = new Serialization();

		Model model = createSimpleModel();
		List<Triple> triples = serializer.serialize(model);
		StringBuilder stringBuilder = new StringBuilder();
		for (Triple triple : triples) {
			stringBuilder.append(triple.getNtriples());
			stringBuilder.append(System.lineSeparator());
		}
		String serializedModel = stringBuilder.toString();

		Model newModel = serializer.deserialize(serializedModel);

		Assert.assertTrue(model.containsAll(newModel));
		Assert.assertTrue(newModel.containsAll(model));
	}

	/**
	 * Tests serialization of a model containing blank nodes and literals.
	 */
	@Test
	public void testAdvanced() throws Exception {
		Serialization serializer = new Serialization();

		Model model = createAdvancedModel();
		List<Triple> triples = serializer.serialize(model);
		StringBuilder stringBuilder = new StringBuilder();
		for (Triple triple : triples) {
			stringBuilder.append(triple.getNtriples());
			stringBuilder.append(System.lineSeparator());
		}
		String serializedModel = stringBuilder.toString();

		Model newModel = serializer.deserialize(serializedModel);

		// Does not work as contains blank nodes
		// Assert.assertTrue(model.containsAll(newModel));
		// Assert.assertTrue(newModel.containsAll(model));

		Resource testSubject = model.listSubjectsWithProperty(predicateA).next();
		Resource testBlank = model.listObjectsOfProperty(testSubject, predicateA).next().asResource();
		RDFNode testLiteral = model.listObjectsOfProperty(testBlank, predicateB).next();
		RDFNode testTyped = model.listObjectsOfProperty(testBlank, predicateC).next();

		Resource testSubjectNew = newModel.listSubjectsWithProperty(predicateA).next();
		Resource testBlankNew = newModel.listObjectsOfProperty(testSubjectNew, predicateA).next().asResource();
		RDFNode testLiteralNew = newModel.listObjectsOfProperty(testBlankNew, predicateB).next();
		RDFNode testTypedNew = newModel.listObjectsOfProperty(testBlankNew, predicateC).next();

		assertTrue(testLiteral.toString().equals(testLiteralNew.toString()));
		assertTrue(Integer.valueOf(testTyped.asLiteral().getInt())
				.equals(Integer.valueOf(testTypedNew.asLiteral().getInt())));
	}

	/**
	 * Was used to test N-Triples serialization.
	 */
	@Test
	public void testNtriplesSimple() {
		Assume.assumeTrue(EXECUTE_DEV_TESTS);

		Model model = createSimpleModel();
		StringWriter stringWriter = new StringWriter();
		model.write(stringWriter, RDFLanguages.strLangNTriples);
		String data = stringWriter.toString();
		// Also possible:
		// RDFDataMgr.write(stringWriter, model, RDFLanguages.NTRIPLES);

		StringReader stringReader = new StringReader(data);
		Model newModel = ModelFactory.createDefaultModel();
		newModel.read(stringReader, "", RDFLanguages.strLangNTriples);
		// Also possible:
		// RDFDataMgr.read(newModel, byteArrayInputStream, RDFLanguages.NTRIPLES);
		// ByteArrayInputStream byteArrayInputStream = new
		// ByteArrayInputStream(data.getBytes());

		Assert.assertTrue(model.containsAll(newModel));
		Assert.assertTrue(newModel.containsAll(model));
	}

	/**
	 * Was used to test N-Triples serialization.
	 */
	@Test
	public void testNtriplesAdvanced() {
		Assume.assumeTrue(EXECUTE_DEV_TESTS);

		Model model = createAdvancedModel();
		StringWriter stringWriter = new StringWriter();
		model.write(stringWriter, RDFLanguages.strLangNTriples);
		String data = stringWriter.toString();
		// Also possible:
		// RDFDataMgr.write(stringWriter, model, RDFLanguages.NTRIPLES);

		StringReader stringReader = new StringReader(data);
		Model newModel = ModelFactory.createDefaultModel();
		newModel.read(stringReader, "", RDFLanguages.strLangNTriples);
		// Also possible:
		// RDFDataMgr.read(newModel, byteArrayInputStream, RDFLanguages.NTRIPLES);
		// ByteArrayInputStream byteArrayInputStream = new
		// ByteArrayInputStream(data.getBytes());

		// Does not work as contains blank nodes
		// Assert.assertTrue(model.containsAll(newModel));
		// Assert.assertTrue(newModel.containsAll(model));

		Resource testSubject = model.listSubjectsWithProperty(predicateA).next();
		Resource testBlank = model.listObjectsOfProperty(testSubject, predicateA).next().asResource();
		RDFNode testLiteral = model.listObjectsOfProperty(testBlank, predicateB).next();
		RDFNode testTyped = model.listObjectsOfProperty(testBlank, predicateC).next();

		Resource testSubjectNew = newModel.listSubjectsWithProperty(predicateA).next();
		Resource testBlankNew = newModel.listObjectsOfProperty(testSubjectNew, predicateA).next().asResource();
		RDFNode testLiteralNew = newModel.listObjectsOfProperty(testBlankNew, predicateB).next();
		RDFNode testTypedNew = newModel.listObjectsOfProperty(testBlankNew, predicateC).next();

		assertTrue(testLiteral.toString().equals(testLiteralNew.toString()));
		assertTrue(Integer.valueOf(testTyped.asLiteral().getInt())
				.equals(Integer.valueOf(testTypedNew.asLiteral().getInt())));
	}

	/**
	 * Was used to select language for serialization.
	 */
	@Test
	public void serializationLanguagesTest() {
		Assume.assumeTrue(EXECUTE_BASIC_LANGUAGE_TESTS);

		for (Lang lang : RDFLanguages.getRegisteredLanguages()) {
			StringWriter stringWriter = new StringWriter();
			try {
				RDFDataMgr.write(stringWriter, createAdvancedModel(), lang);
			} catch (RiotException e) {
				// Prints: No output format for Lang:...
				System.err.println(lang + " " + e);
			}
			System.out.println(lang);
			System.out.println();
			System.out.println(stringWriter.toString());
			System.out.println();
			System.out.println();
		}
	}

}

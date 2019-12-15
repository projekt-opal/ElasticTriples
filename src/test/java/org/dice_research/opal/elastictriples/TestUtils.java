package org.dice_research.opal.elastictriples;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;

public abstract class TestUtils {

	public static void main(String[] args) throws FileNotFoundException {

		// Convert file
		if (Boolean.FALSE) {
			File inputFile = new File("/tmp/test.ttl");
			String inputLang = "Turtle";
			File outputFile = new File("/tmp/test.nt");
			String outputLang = "N-Triples";
			convertFile(inputFile, inputLang, outputFile, outputLang);
		}

		// Create N-Triples resource file
		if (Boolean.FALSE) {
			createNtResourceFile();
		}

	}

	public static void convertFile(File inputFile, String inputLang, File outputFile, String outputLang)
			throws FileNotFoundException {
		long time = System.currentTimeMillis();
		Model model = RDFDataMgr.loadModel(inputFile.toURI().toString(), RDFLanguages.nameToLang(inputLang));
		RDFDataMgr.write(new FileOutputStream(outputFile), model, RDFLanguages.nameToLang(outputLang));
		System.out.println("Convert time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}

	public static void createNtResourceFile() throws FileNotFoundException {
		File file = new File("src/test/resources/triples.nt");
		RDFDataMgr.write(new FileOutputStream(file), new SerializationTest().createAdvancedModel(),
				RDFLanguages.nameToLang("N-Triples"));
	}

}

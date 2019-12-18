package org.dice_research.opal.elastictriples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;

public abstract class TestUtils {

	public static void main(String[] args) throws Exception {

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

		// Remove redundancy, filter languages
		if (Boolean.FALSE) {
			removeRendundantLines(new File("/tmp/opal-rdf/edp.nt"), new File("/tmp/opal-rdf/edp-filtered.nt"), true);
		}

		// Test regex
		if (Boolean.FALSE) {
			String line = "<https://europeandataportal.eu/set/distribution/6d4f1461-9df6-4408-8092-4bf85c7aa434> <http://purl.org/dc/terms/description> \"Interwzum czorcäuden päuden/Tewäuden/Teacyina i 6 gäpäu3D-Formaten\\n\"@no-t-en-t0-mtec .";
			Pattern pattern = Pattern.compile("@([a-z]{2})(-t-[a-z]{2}-t0-mtec)?[ ]\\.$");
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				System.out.println(matcher.group(1));
			}
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

	/**
	 * Filters N-Triple file with 74,700,000 triples in 280 seconds.
	 */
	public static void removeRendundantLines(File inFile, File outFile, boolean filterLang)
			throws FileNotFoundException, IOException {
		long time = System.currentTimeMillis();
		long counter = 0;
		Matcher matcher;
		String language;
		// "Literal"@en-t-cs-t0-mtec .
		// "Literal"@en .
		Pattern pattern = Pattern.compile("@([a-z]{2})(-t-[a-z]{2}-t0-mtec)?[ ]\\.$");
		String parentLine = new String();
		try (FileWriter fw = new FileWriter(outFile)) {
			try (BufferedWriter bw = new BufferedWriter(fw)) {
				try (BufferedReader br = new BufferedReader(new FileReader(inFile))) {
					for (String line; (line = br.readLine()) != null;) {

						// Redundancy
						if (!line.equals(parentLine)) {

							// Language filtering
							if (filterLang) {
								matcher = pattern.matcher(line);
								if (matcher.find()) {
									language = matcher.group(1);
									if (!language.equals("de") && !language.equals("en")) {
										continue;
									}
								}
							}

							bw.write(line);
							bw.write(System.lineSeparator());
							parentLine = line;
						}

						if (counter % 100000 == 0) {
							System.out.println(counter);
						}
						counter++;
					}
				}
			}
		}
		System.out.println("Time: " + (System.currentTimeMillis() - time) / 1000f + " seconds");
	}

}

package eu.europeana.downloads;

import eu.europeana.api.commons.utils.TurtleRecordWriter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class TurtleResponseParser {

    private static final Logger LOG = LogManager.getLogger(TurtleResponseParser.class);

    private TurtleResponseParser() {
        //adding a private constructor to hide implicit public one
    }

    public static String generateTurtle(String data) {
        try (OutputStream outputStream = new ByteArrayOutputStream();
             InputStream rdfInput = new ByteArrayInputStream(data.getBytes());
             TurtleRecordWriter writer = new TurtleRecordWriter(outputStream)) {
            Model modelResult = ModelFactory.createDefaultModel().read(rdfInput, "", Constants.RDF_XML);
            writer.write(modelResult);
            return outputStream.toString();
        } catch (IOException e) {
            LOG.error("Error generating turtle output", e);
        }
        return "";
    }
}

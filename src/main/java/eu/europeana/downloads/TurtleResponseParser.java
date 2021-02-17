package eu.europeana.downloads;

import eu.europeana.api.commons.utils.TurtleRecordWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RiotException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    //TODO remove the temp code once the issue is fixed. See EA-2397 and EA-2066
    /**
     * Temporary fix for Riot Exception for Bad URI in the metadata
     * fetches all the uri present in the data which starts with "http:// or "https://
     * filters the uri which have spaces in them
     * later replaces trims the uri and return the clean data
     *
     * @param data data to be cleaned
     * @return String cleaned data
     */
    public static String getCleanedData(String data) {
        // get all the urls with space in them
        List<String> urls = getURIWithSpace(data);
        String newValue = null;
        for (String url : urls) {
            // get the replacement
            String replacement = StringUtils.replaceChars(url, " ", "");
            newValue = StringUtils.replace(data, url, replacement);
            data = newValue;
        }
        return newValue;
    }

    /**
     * Gets all the uri's with space in them
     * @param data
     * @return List<String>
     */
    private static List<String> getURIWithSpace(String data) {
        List<String> httpUrls = new ArrayList<>();
        List<String> httpsUrls = new ArrayList<>();
        if (data.contains("http://")) {
            httpUrls = Arrays.asList(StringUtils.substringsBetween(data, "\"http://", "\""));
        }
        if (data.contains("https://")) {
            httpsUrls = Arrays.asList(StringUtils.substringsBetween(data, "\"https://", "\""));
        }
        // combine and filter the url with space
        return Stream.of(httpUrls, httpsUrls)
                .flatMap(x -> x.stream().filter(url -> url.contains(" ")))
                .collect(Collectors.toList());
    }
}

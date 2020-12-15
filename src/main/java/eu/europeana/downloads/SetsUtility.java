package eu.europeana.downloads;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SetsUtility {

    private static final Logger LOG = LogManager.getLogger(SetsUtility.class);

    private SetsUtility() {
        //adding a private constructor to hide implicit public one
    }

    /**
     * Reads the lastHarvest date
     *
     * @param path path of the file
     * @return lastharvestDate or " " if not present
     */
    public static String getLastHarvestDate(String path) {
        try {
            return Files.readString(Paths.get(path));
        } catch (NoSuchFileException e) {
            LOG.error("{} file doesn't exist. Harvesting ALL sets for first time" , Constants.HARVEST_DATE_FILENAME);
        } catch (IOException e) {
                LOG.error("Error reading the lastHarvestDate file", e);
        }
        return "";
    }

    /**
     * rewrites the lastHarvestDate file with the new date of harvest
     *
     * @param directoryLocation folder location where file is present
     * @param startTime         start time of Downloads
     */
    public static void writeNewHarvestDate(String directoryLocation, long startTime) {
        try (PrintWriter writer = new PrintWriter(directoryLocation + Constants.PATH_SEPERATOR + Constants.HARVEST_DATE_FILENAME, StandardCharsets.UTF_8)) {
            DateFormat formatter = new SimpleDateFormat(Constants.HARVEST_DATE_FORMAT);
            writer.write((formatter.format(new Date(startTime))));
        } catch (IOException e) {
            LOG.error("Error writing the {} file ", Constants.HARVEST_DATE_FILENAME, e);
        }
    }

    /**
     * gets the list of last harvested datasets
     *
     * @return list of all datasets (ie; .zip files) harvested last time
     */
    public static List<String> getLastHarvestedSets(String directoryLocation) {
        List<String> result = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(Paths.get(directoryLocation))) {
            result = walk.map(x -> StringUtils.substring(x.toString(), directoryLocation.length() + 1))
                    .filter(f -> f.endsWith(Constants.ZIP_EXTENSION)).collect(Collectors.toList());
            // remove  .zip from the values
            result = result.stream().map(s -> s.replaceAll(Constants.ZIP_EXTENSION, ""))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOG.error("Error getting the details of last harvested sets ", e);
        }
        return result;
    }

    /**
     * gets the list of depublished sets.
     * Compares with the existing list of dataset with the previously downloaded list of dataset
     *
     * @return list of all depublished dataset
     */
    public static List<String> getSetsToBeDeleted(OAIPMHServiceClient oaipmhServer, String directoryLocation, int logProgressInterval) {
        LOG.info("Executing ListSet for De-published Datasets ");
        ListSetsQuery setsQuery = new ListSetsQuery(logProgressInterval);
        List<String> lastHarvestedSets = getLastHarvestedSets(directoryLocation);
        List<String> currentSetsList = setsQuery.getSets(oaipmhServer, null, null);
        //remove all the sets which are present in server and are available in database
        lastHarvestedSets.removeAll(currentSetsList);
        return lastHarvestedSets;
    }

    /**
     * Deletes the list of sets provided.
     * Deletes .zip file and .md5sum file for that set
     *
     * @param directoryLocation folder location where file is present
     */
    public static void deleteDataset(List<String> setsToBeDeleted, String directoryLocation) {
        for (String set : setsToBeDeleted) {
            try {
                LOG.info("Deleting set : {} ", set);
                String fileName = directoryLocation + Constants.PATH_SEPERATOR + set + Constants.ZIP_EXTENSION;
                Path zipFile = Paths.get(fileName);
                Path md5sumFile = Paths.get(fileName + Constants.MD5_EXTENSION);
                Files.delete(zipFile);
                Files.delete(md5sumFile);
            } catch (IOException e) {
               LOG.error("Error deleting file {} ", set, e);
            }
        }
    }

    // returns the current date in HARVEST_DATE_FORMAT
    public static String getDate() {
        DateFormat formatter = new SimpleDateFormat(Constants.HARVEST_DATE_FORMAT);
        Date today = Calendar.getInstance().getTime();
        return formatter.format(today);
    }

    /**
     * Creates the folders
     * for TTL : directoryLocation/TTL
     * For XML : directoryLocation/XML
     *
     * @param directoryLocation folder location where file is present
     */
    public static void createFolders(String directoryLocation) {
        File ttlDirectory = new File(directoryLocation, Constants.TTL_FILE);
        File xmlDirectory = new File(directoryLocation, Constants.XML_FILE);
        if (! ttlDirectory.exists()) {
            ttlDirectory.mkdir();
       }
        if (!xmlDirectory.exists()) {
            xmlDirectory.mkdir();
        }
    }

    /**
     *  retruns the folder path based on file format
     * for TTL : directoryLocation/TTL
     * For XML : directoryLocation/XML
     *
     * @param directoryLocation folder location where file is present
     * @param fileFormat file format
     */
    public static String getFolderName(String directoryLocation, String fileFormat) {
        if(StringUtils.equals(fileFormat, Constants.TTL_FILE)) {
            return directoryLocation + Constants.PATH_SEPERATOR + Constants.TTL_FILE;
        }
        return directoryLocation + Constants.PATH_SEPERATOR + Constants.XML_FILE;
    }

    /**
     * Returns the sets along with their status
     *
     * @param retriedSets The sets retried from the FailedSets.csv file
     * @param directoryLocation folder location where file is present
     */
    public static String getRetriedSetsStatus(List<String> retriedSets, String directoryLocation){
        StringBuilder status = new StringBuilder();
        List<String> failedSets = CSVFile.readCSVFile(CSVFile.getCsvFilePath(directoryLocation));
        status.append("\n");
        if (failedSets.isEmpty()){
            status.append(retriedSets);
            status.append(": ");
            status.append(Constants.SUCCESS);
        } else {
            retriedSets.removeAll(failedSets);
            if (!retriedSets.isEmpty()) {
                status.append(retriedSets);
                status.append(": ");
                status.append(Constants.SUCCESS);
                status.append("\n");
            }
            status.append(failedSets);
            status.append(": ");
            status.append(Constants.FAILED);
        }
        return status.toString();
    }
}

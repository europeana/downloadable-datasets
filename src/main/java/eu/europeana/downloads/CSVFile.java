package eu.europeana.downloads;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class CSVFile {

    private static final Logger LOG = LogManager.getLogger(CSVFile.class);

    private CSVFile() {
        //adding a private constructor to hide implicit public one
    }

    // creates a csv file in the desired location
    public static void writeInCsv(List<String> sets, String directoryLocation) {
        String fileName = directoryLocation + Constants.PATH_SEPERATOR + Constants.CSV_FILE + "_" + getDate() + Constants.CSV_EXTENSION;
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), StandardCharsets.UTF_8))) {
            String header = Constants.CSV_HEADER;
            bw.write(header);
           bw.newLine();
            for (String set : sets) {
                bw.write(set);
                bw.newLine();
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("Unsupported encoding format", e);
        } catch (FileNotFoundException e) {
            LOG.error("Error creating the file ", e);
        } catch (IOException e) {
            LOG.error("Error creating csv file", e);
        }
    }

    // returns the current date
    private static String getDate() {
        Date today = Calendar.getInstance().getTime();
        DateFormat formatter = new SimpleDateFormat(Constants.REPORT_DATE_FORMAT);
        return formatter.format(today);
    }

    // returns the CSV file path
    public static String getCsvFilePath(String directoryLocation) {
       return directoryLocation + Constants.PATH_SEPERATOR + Constants.CSV_FILE + "_" + getDate() + Constants.CSV_EXTENSION;
    }

    /**
     * Reads the FailedSetsReport generated at that time
     *
     * @param path path of the file
     * @return list of all the failed sets
     * returns "", if file is empty.
     */
    public static List<String> readCSVFile(String path) {
        List<String> failedSets = new ArrayList<>();
        try(BufferedReader csvReader = new BufferedReader(new FileReader(path))) {
            String  nextLine;
            while ((nextLine = csvReader.readLine()) != null) {
                if (!StringUtils.equals(nextLine, Constants.CSV_HEADER)) {
                    failedSets.add(nextLine);
                }
            }
        } catch (FileNotFoundException e) {
            LOG.error("{} file doesn't exist. Failed sets report not generated" , Constants.CSV_FILE);
        } catch (IOException e) {
            LOG.error("Error reading the {} file",Constants.CSV_FILE, e);
        }
        return failedSets;
    }
}


package eu.europeana.downloads;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.zip.ZipOutputStream;

/**
 * This is an alternative approach. Currently we are not using it.
 * @deprecated since 15 July 2020
 */
@Deprecated
public class ListIdentifierExecutor implements Callable<ListRecordsResult> {

    private static final Logger LOG = LogManager.getLogger(ListIdentifierExecutor.class);
    private static final int MAX_ERRORS_PER_THREAD = 5;

    private static ProgressLogger logger = null;

    private static long loggerThreadId;
    private String metadataPrefix;
    private String from;
    private List<String> sets;
    private OAIPMHServiceClient oaipmhServer;
    private String directoryLocation;
    private int logProgressInterval;

    public ListIdentifierExecutor(List<String> sets, String from, String metadataPrefix, String directoryLocation, OAIPMHServiceClient oaipmhServer, int logProgressInterval) {
        this.sets = sets;
        this.from = from;
        this.metadataPrefix = metadataPrefix;
        this.directoryLocation = directoryLocation;
        this.oaipmhServer = oaipmhServer;
        this.logProgressInterval = logProgressInterval;
    }

    @Override
    public ListRecordsResult call() {
        int errors = 0;
        long counter = 0;
        long start = System.currentTimeMillis();

//         This is a bit of a hack. The first callable that reaches this point will create a progressLogger and only
//         this callable will log progress. This is to avoid too much logging from all threads.
        synchronized (this) {
            if (logger == null) {
                logger = new ProgressLogger("ListIdentifiers Sets", sets.size(), logProgressInterval);
                loggerThreadId = Thread.currentThread().getId();
                LOG.debug("Created new progress logger for thread {} - {} items, logging interval {} ms",
                        loggerThreadId, sets.size(), logProgressInterval);
            }
        }
        for (String set : sets) {
            try {
                ListIdentifiersQuery identifiersQuery = new ListIdentifiersQuery(metadataPrefix, from, set, logProgressInterval);
                identifiersQuery.initSets();
                List<String> identifiers = identifiersQuery.getIdentifiers(oaipmhServer, set);
                // start downloading the collected identifiers
                errors += harvestIdentifiers(identifiers, set);
            } catch (Exception e) {
                LOG.error("Error collecting identifiers for set {}", set, e);
                errors++;
            }
            // if there are too many errors, just abort
            if (errors > MAX_ERRORS_PER_THREAD) {
                LOG.error("Terminating getIdentifiers thread {} because too many errors occurred", Thread.currentThread().getId());
                break;
            }
            if (Thread.currentThread().getId() == loggerThreadId) {
                counter++;
                logger.logProgress(counter);
            }

        }
        return new ListRecordsResult((System.currentTimeMillis() - start) / 1000F, errors);
    }

    /**
     * Method to download the records. Creates a zip folder with the setname
     * in the specified location and generates md5 sum file for the zip.
     *
     * @param identifiers list of identifiers
     * @param setIdentifier setName
     * @return number of errors occurred during the process to terminate the thread in progress.
     */
    private int harvestIdentifiers(List<String> identifiers, String setIdentifier) {
        int errors = 0;
        long start = System.currentTimeMillis();
        String zipName =  directoryLocation + Constants.PATH_SEPERATOR + setIdentifier + Constants.ZIP_EXTENSION;

        try (ZipOutputStream zout = new ZipOutputStream(new FileOutputStream(new File(zipName)));
             OutputStreamWriter writer = new OutputStreamWriter(zout)) {

            for (String identifier : identifiers) {
                try {
                    new GetRecordQuery(metadataPrefix, identifier, zout, writer).execute(oaipmhServer);
                } catch (Exception e) {
                    LOG.error("Error retrieving identifier {} for set {} ", identifier, setIdentifier, e);
                    errors++;
                    // if there are too many errors, just abort
                    if (errors > MAX_ERRORS_PER_THREAD) {
                        LOG.error("Terminating GetRecord thread {} because too many errors occurred", Thread.currentThread().getId());
                        break;
                    }
                }
            }
            LOG.info("ListIdentifiers for set " + setIdentifier + " executed in " + ProgressLogger.getDurationText(System.currentTimeMillis() - start) +
                    ". Harvested " + identifiers.size() + " identifiers.");
        } catch (IOException e) {
            LOG.error("Error creating zip file ", e);
            errors++;
        }
        //create MD5Sum file for the Zip
        ZipUtility.createMD5SumFile(zipName);
        return errors;
    }
}

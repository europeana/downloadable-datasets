package eu.europeana.downloads;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.util.List;
import java.util.concurrent.Callable;

public class ListSetsExecutor implements Callable<ListRecordsResult> {

    private static final Logger LOG = LogManager.getLogger(ListSetsExecutor.class);

    private static final int MAX_ERRORS_PER_THREAD = 5;

    private static final int MAX_RETRIES_PER_THREAD = 2;


    private static ProgressLogger logger = null;
    private static long loggerThreadId;
    private int logProgressInterval;

    private List<String> sets;

    private String directoryLocation;

    private String metadataPrefix;

    private OAIPMHServiceClient oaipmhServer;


    public ListSetsExecutor(List<String> sets, String metadataPrefix, String directoryLocation, OAIPMHServiceClient oaipmhServer, int logProgressInterval) {
        this.sets = sets;
        this.metadataPrefix = metadataPrefix;
        this.directoryLocation = directoryLocation;
        this.oaipmhServer = oaipmhServer;
        this.logProgressInterval = logProgressInterval;
    }

    @Override
    public ListRecordsResult call() throws Exception {
        int errors = 0;
        long counter = 0;
        long start = System.currentTimeMillis();
        StringBuilder setsDownloaded= new StringBuilder();
        // This is a bit of a hack. The first callable that reaches this point will create a progressLogger and only
        // this callable will log progress. This is to avoid too much logging from all threads.
        synchronized (this) {
            if (logger == null) {
                logger = new ProgressLogger("List sets", sets.size(), logProgressInterval);
                loggerThreadId = Thread.currentThread().getId();
                LOG.debug("Created new progress logger for thread {} - {} items, logging interval {} ms",
                        loggerThreadId, sets.size(), logProgressInterval);
            }
        }
        Map<String, String> failedRecordCountPerSet = new HashMap<String, String>();
        for (String set : sets) {
            ListRecordsQuery listRecordsQuery = new ListRecordsQuery(metadataPrefix, set,
                directoryLocation, logProgressInterval);
            try {
                listRecordsQuery.execute(oaipmhServer);
                setsDownloaded.append(set).append(",");
            } catch (HttpServerErrorException | ResourceAccessException e) {
                LOG.error("Error retrieving set {} {}", set, e);
                // thread to wait for 2 milliseconds
                Thread.sleep(2000);
                // will retry the request
                boolean retrySuccess = retryTask(set);
                if (! retrySuccess) {
                    errors++;
                }
            }

            catch (Exception e) {
                LOG.error("Error retrieving set {} {}", set, e);
                errors++;
            }
            // if there are too many errors, just abort
            if (errors > MAX_ERRORS_PER_THREAD) {
                LOG.error("Terminating ListRecords thread {} because too many errors occurred", Thread.currentThread().getId());
                break;
            }
            if (Thread.currentThread().getId() == loggerThreadId) {
                counter++;
                logger.logProgress(counter);
            }
            long failedRecordCount = Long.valueOf(listRecordsQuery.recordsTobeDownloaded)- listRecordsQuery.recordsDownloaded;
            failedRecordCountPerSet.put(set,String.valueOf(failedRecordCount));
        }
        return new ListRecordsResult((System.currentTimeMillis() - start) / 1000F, setsDownloaded.toString(), errors,failedRecordCountPerSet);
    }

    /**
     * retry mechanism : if error is due to connection issues with oai-pmh application
     * request will be retried  MAX_RETRIES_PER_THREAD times.
     *
     * @param set set to be executed
     * @return boolean if retry is successful
    */
    private boolean retryTask(String set) {
        boolean success = false;
        for (int i = 1; i <= MAX_RETRIES_PER_THREAD; i++) {
            if (!success) {
                try {
                    LOG.info("Retrying the set {} {} times ", set, i);
                    new ListRecordsQuery(metadataPrefix, set, directoryLocation, logProgressInterval).execute(oaipmhServer);
                    success = true;
                } catch (HttpServerErrorException | ResourceAccessException ex) {
                    if (i == MAX_RETRIES_PER_THREAD) {
                        LOG.error("Error retrieving set {} after {} retries {}", set, i, ex);
                    }
                } catch (Exception ex) {
                    LOG.error("Error retrieving set {} after {} retries {}", set, i, ex);
                    break;
                }
            }
        }
        return success;
    }
}

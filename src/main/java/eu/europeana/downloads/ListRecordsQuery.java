package eu.europeana.downloads;

import eu.europeana.oaipmh.model.ListRecords;
import eu.europeana.oaipmh.model.Record;
import eu.europeana.oaipmh.model.response.ListRecordsResponse;
import eu.europeana.oaipmh.service.exception.OaiPmhException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.ZipOutputStream;

@Component
public class ListRecordsQuery extends BaseQuery implements OAIPMHQuery {

    private static final Logger LOG = LogManager.getLogger(ListRecordsQuery.class);

    @Value("${metadata-prefix}")
    private String metadataPrefix;

    @Value("${harvest-from}")
    private String from;

    @Value("${harvest-sets}")
    private String set;

    @Value("${log-progress-interval}")
    private Integer logProgressInterval;

    @Value("${sets-folder}")
    private String directoryLocation;

    @Value("${file-format}")
    private String fileFormat;

    @Value("${harvest-threads}")
    private int threads;

    private String lastHarvestDate;

    private List<String> sets = new ArrayList<>();

    private ExecutorService threadPool;

    @Autowired
    private MailService  emailService;

    @Autowired
    private SimpleMailMessage downloadsReportMail;

    public ListRecordsQuery() {
    }

    public ListRecordsQuery(String metadataPrefix, String set, String directoryLocation, String fileFormat, int logProgressInterval) {
        this.metadataPrefix = metadataPrefix;
        this.set = set;
        this.directoryLocation = directoryLocation;
        this.fileFormat = fileFormat;
        this.logProgressInterval = logProgressInterval;
    }

    @PostConstruct
    public final void initSets() {
        lastHarvestDate = SetsUtility.getLastHarvestDate(directoryLocation + Constants.PATH_SEPERATOR + Constants.HARVEST_DATE_FILENAME);
        if (set != null && !set.isEmpty() && !StringUtils.equals(set, "ALL")) {
            sets.addAll(Arrays.asList(set.split(",")));
        }
    }

    private void initThreadPool() {
        // init thread pool
        if (threads < 1) {
            threads = 1;
        }
        threadPool = Executors
                .newFixedThreadPool(threads);
    }

    @Override
    public String getVerbName() {
        return Constants.LIST_RECORDS_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer, List<String> failedSets) throws OaiPmhException {
        // if failedSets are present, download them
        if (failedSets != null && !failedSets.isEmpty()) {
            DownloadsStatus status = executeMultithreadListRecords(oaipmhServer, failedSets, "");
            sendEmail(status, true);
        }
        else if (sets.size() != 1 && threads > 1) {
            DownloadsStatus status = executeMultithreadListRecords(oaipmhServer, sets, lastHarvestDate);
            sendEmail(status, false);
        } else {
            executeListRecords(oaipmhServer, set, fileFormat);
        }
    }

    /**
     * Method will send email with the download status
     * @param status
     * @param retryMail true : if the mail is sent for failed sets retry.
     */
    private void sendEmail(DownloadsStatus status, boolean retryMail){
        LOG.info("Sending email ");
        String subject = retryMail ? Constants.FAILED_SETS_RETRY_SUBJECT : Constants.DOWNLOADS_SUBJECT;
        emailService.sendSimpleMessageUsingTemplate(subject,
                downloadsReportMail,
                String.valueOf(status.getNoOfSets()),
                String.valueOf(status.getStartTime()),
                status.getTimeElapsed(),
                String.valueOf(status.getSetsHarvested()));
    }

    private DownloadsStatus executeMultithreadListRecords(OAIPMHServiceClient oaipmhServer, List<String> sets, String lastHarvestDate) {
        initThreadPool();
        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger("Multiple sets", -1, logProgressInterval);
        // make a deep copy of sets
        List<String> setsFromListSets = new ArrayList<>();
        Iterator<String> iterator = sets.iterator();
        while(iterator.hasNext()){
            setsFromListSets.add(iterator.next());
        }
        // if setsFromListSets is still empty get the sets from ListSet
        // ie; either set is set to ALL or empty
        if (setsFromListSets.isEmpty()) {
            setsFromListSets = getSetsFromListSet(oaipmhServer, setsFromListSets, lastHarvestDate);
        }
        DownloadsStatus status = new DownloadsStatus(setsFromListSets.size(), 0, new java.util.Date(start));

        if (! setsFromListSets.isEmpty()) {
            logger.setTotalItems(setsFromListSets.size());
            List<Future<ListRecordsResult>> results = null;
            List<Callable<ListRecordsResult>> tasks = new ArrayList<>();

            int perThread = setsFromListSets.size() / threads;

        // create task for each resource provider
        for (int i = 0; i < threads; i++) {
            int fromIndex = i * perThread;
            int toIndex = (i + 1) * perThread;
            if (i == threads - 1) {
                toIndex = setsFromListSets.size();
            }
            tasks.add(new ListSetsExecutor(setsFromListSets.subList(fromIndex, toIndex), metadataPrefix, directoryLocation, fileFormat, oaipmhServer, logProgressInterval));
        }
        try {
            // invoke a separate thread for each provider
            results = threadPool.invokeAll(tasks);
            List<String> setsDownloaded = new ArrayList<>();
                ListRecordsResult listRecordsResult;
                for (Future<ListRecordsResult> result : results) {
                    listRecordsResult = result.get();
                    LOG.info("Executor finished with {} errors in {} sec.",
                            listRecordsResult.getErrors(), listRecordsResult.getTime());
                    counter += perThread;
                    // get the successfully downloaded sets
                if(StringUtils.isNotEmpty(listRecordsResult.getSetsDownloaded())) {
                    setsDownloaded.addAll(Arrays.asList(listRecordsResult.getSetsDownloaded().split("\\s*,\\s*")));
                }
                logger.logProgress(counter);
            }
            status.setSetsHarvested(setsDownloaded.size());
            getFailedSets(setsFromListSets, setsDownloaded,directoryLocation);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted.", e);
            } catch (ExecutionException e) {
                LOG.error("Problem with task thread execution.", e);
            }
        }

        // store the new harvest start date in the file
        // Currently not changing the lastHarvestDate if failed-sets or manually added sets are running
        if(sets.isEmpty()) {
            LOG.info("Creating/Updating the {} file ", Constants.HARVEST_DATE_FILENAME);
            SetsUtility.writeNewHarvestDate(directoryLocation, start);
        }
        clean();
        String timeElapsed = ProgressLogger.getDurationText(System.currentTimeMillis() - start);
        status.setTimeElapsed(timeElapsed);
        LOG.info("ListRecords for all {} sets executed in {}. Harvested {} sets." ,status.getNoOfSets(), timeElapsed, status.getSetsHarvested() );
        return status;
    }


    /**
     * gets the list of sets to be execeuted
     * if set == ALL, gets all the sets present
     * if set is Empty and lastHarvestDate is present, gets all the updated, newly created and de-published datasets
     * Also deletes the de-published datasets
     *
     * @return list of  datasets
     */
    private List<String> getSetsFromListSet (OAIPMHServiceClient oaipmhServer, List<String> setsFromListSets, String lastHarvestDate) {
        ListSetsQuery setsQuery = new ListSetsQuery(logProgressInterval);
        // Harvest all datasets if set = "ALL"
        if (StringUtils.equals(set, "ALL")) {
            setsFromListSets = setsQuery.getSets(oaipmhServer, null, null);
            LOG.info("ALL {} sets ready for harvest.", setsFromListSets.size());
        }
        // if set is empty and lastHarvestDate is present. Check for Updated, newly created and de-published datasets
        else if (set.isEmpty() && ! lastHarvestDate.isEmpty() ) {
            List<String> setsToBeDeleted = SetsUtility.getSetsToBeDeleted(oaipmhServer, directoryLocation, logProgressInterval);
            // delete the de-published datasets
            if (! setsToBeDeleted.isEmpty()) {
                LOG.info("De-published datasets : {} ", setsToBeDeleted.size());
                SetsUtility.deleteDataset(setsToBeDeleted, directoryLocation);
            }
            //get the updated or newly added datasets
            setsFromListSets = setsQuery.getSets(oaipmhServer, lastHarvestDate, null);
            LOG.info("Updated or newly created datasets count is {} ", setsFromListSets.size());
        }
        return setsFromListSets;
    }

    private void executeListRecords(OAIPMHServiceClient oaipmhServer, String setIdentifier, String fileFormat) {
        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger( setIdentifier, -1, logProgressInterval);

        String request = getRequest(oaipmhServer.getOaipmhServer(), setIdentifier);
        ListRecordsResponse response = oaipmhServer.getListRecordRequest(request);
        ListRecords responseObject = response.getListRecords();
        String zipName = SetsUtility.getFolderName(directoryLocation, fileFormat) +
                Constants.PATH_SEPERATOR + setIdentifier + Constants.ZIP_EXTENSION ;
        try (final ZipOutputStream zout = new ZipOutputStream(new FileOutputStream(new File(zipName)));
             OutputStreamWriter writer = new OutputStreamWriter(zout)) {

              //writing in ZIP
              for(Record record : responseObject.getRecords()) {
                    ZipUtility.writeInZip(zout, writer, record, fileFormat);
              }
             if (responseObject != null) {
                counter += responseObject.getRecords().size();

                if (responseObject.getResumptionToken() != null) {
                    logger.setTotalItems(responseObject.getResumptionToken().getCompleteListSize());
                } else {
                    logger.setTotalItems(responseObject.getRecords().size());
                }
                while (responseObject.getResumptionToken() != null) {

                    request = getResumptionRequest(oaipmhServer.getOaipmhServer(), responseObject.getResumptionToken().getValue());
                    response = oaipmhServer.getListRecordRequest(request);
                    responseObject = response.getListRecords();
                        //writing in ZIP
                        for (Record record : responseObject.getRecords()) {
                            ZipUtility.writeInZip(zout, writer, record, fileFormat);
                    }
                    if (responseObject == null) {

                        break;
                    }
                    counter += responseObject.getRecords().size();
                    logger.logProgress(counter);
                }
            }

        } catch (IOException e) {
            LOG.error("Error creating outputStreams ", e);
        }
        //create MD5Sum file for the Zip
        ZipUtility.createMD5SumFile(zipName);
        LOG.info("ListRecords for set {} executed in {}. Harvested {} records.", setIdentifier,
                ProgressLogger.getDurationText(System.currentTimeMillis() - start), counter);
    }

    private String getResumptionRequest(String oaipmhServer, String resumptionToken) {
        return getBaseRequest(oaipmhServer, getVerbName()) +
                String.format(RESUMPTION_TOKEN_PARAMETER, resumptionToken);
    }

    private String getRequest(String oaipmhServer, String setIdentifier) {
        StringBuilder sb = new StringBuilder();
        sb.append(getBaseRequest(oaipmhServer, getVerbName()));
        sb.append(String.format(METADATA_PREFIX_PARAMETER, metadataPrefix));
        if (from != null && !from.isEmpty()) {
            sb.append(String.format(FROM_PARAMETER, from));
        }
        if (set != null && !set.isEmpty() && !StringUtils.equals(set, "ALL")) {
            sb.append(String.format(SET_PARAMETER, setIdentifier));
        }
        return sb.toString();
    }

    // gets the value of failed or missing sets or partially downloaded sets. Writes the data in .csv file
    // It will easier to identify the failed sets after a run
    private static void getFailedSets(List<String> sets, List<String> setsDownloaded, String directoryLocation) {
        sets.removeAll(setsDownloaded);
        CSVFile.writeInCsv(sets,directoryLocation);
    }

    @PreDestroy
    private void clean() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }
}

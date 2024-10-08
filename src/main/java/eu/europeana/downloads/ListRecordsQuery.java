package eu.europeana.downloads;

import eu.europeana.oaipmh.model.ListRecords;
import eu.europeana.oaipmh.model.Record;
import eu.europeana.oaipmh.model.response.ListRecordsResponse;
import eu.europeana.oaipmh.service.exception.OaiPmhException;
import java.util.Map.Entry;
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

    @Value("${harvest-threads}")
    private int threads;

    @Value("${server-url}")
    private String downloadServerURL;

    private String lastHarvestDate;

    private Date currentHarvestStartTime;

    private List<String> sets = new ArrayList<>();

    private ExecutorService threadPool;

    @Autowired
    private MailService  emailService;

    @Autowired
    private SimpleMailMessage downloadsReportMail;

    @Autowired
    private StatusReportService statusReportService;

    public String recordsTobeDownloaded ="0";

    public Integer recordsDownloaded =0;

    public ListRecordsQuery() {
    }

    public ListRecordsQuery(String metadataPrefix, String set, String directoryLocation, int logProgressInterval) {
        this.metadataPrefix = metadataPrefix;
        this.set = set;
        this.directoryLocation = directoryLocation;
        this.logProgressInterval = logProgressInterval;
    }

    @PostConstruct
    public final void initSets() {
        lastHarvestDate = (SetsUtility.getLastHarvestDate(directoryLocation + Constants.PATH_SEPERATOR
                + Constants.HARVEST_DATE_FILENAME, set)).trim();
        if (! set.isEmpty() && !StringUtils.equals(set, "ALL")) {
            sets.addAll(Arrays.asList(set.split(",")));
        }
        currentHarvestStartTime = new Date();
    }

    /**
     * initiates thread pool
     * if threads <1 ; threads = 1
     * For selective Update :
     * if no Of sets to be harvested <= no of threads ; threads = no Of sets
     * this is done to optimise the number of threads, when selective update will run.
     * if No of sets are 0 the thread count is 1.
     *
     * Example : If there are 5 sets to be harvested and threads are 30.
     * Having 5 threads to harvest each one of them would be faster
     * than one thread, to harvest all 5.
     * @param noOfSets -
     */
    private void initThreadPool(int noOfSets, boolean selectiveUpdate) {
        // init thread pool
        if (threads < 1) {
            threads = 1;
        }
        // optimise threads only for selective Update
        if (selectiveUpdate && noOfSets < threads) {
            LOG.info("No Of Sets to be harvested is less than the configured threads. Sets size : {}. Threads : {} ", noOfSets, threads);
            threads = noOfSets;
            // when there are no sets to update
            if (noOfSets < 1) {
                threads = 1;
            }
            LOG.info("Optimised the thread count to {} ", threads);
        }
        threadPool = Executors
                .newFixedThreadPool(threads);
    }

    @Override
    public String getVerbName() {
        return Constants.LIST_RECORDS_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer) throws OaiPmhException {
         if (sets.size() != 1 && threads > 1) {
            DownloadsStatus status = executeMultithreadListRecords(oaipmhServer, sets, lastHarvestDate,currentHarvestStartTime);
            sendEmail(status, false);
        } else {
            executeListRecords(oaipmhServer, set);
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
        String setsHarvested = retryMail ? status.getRetriedSetsStatus() : String.valueOf(status.getSetsHarvested());
        emailService.sendSimpleMessageUsingTemplate(subject,
                downloadsReportMail,
                String.valueOf(status.getNoOfSets()),
                String.valueOf(status.getStartTime()),
                status.getTimeElapsed(),
                String.valueOf(setsHarvested),
                SetsUtility.getTabularData(status));
        statusReportService.publishStatusReport(status,subject,directoryLocation,downloadServerURL);
    }

    private DownloadsStatus executeMultithreadListRecords(OAIPMHServiceClient oaipmhServer, List<String> sets, String lastHarvestDate,Date currentHarvestStartTime) {
        long counter = 0;
        long start = System.currentTimeMillis();
        boolean selectiveUpdate = false;
        ProgressLogger logger = new ProgressLogger("Multiple sets", -1, logProgressInterval);
        // make a deep copy of sets
        List<String> setsFromListSets = new ArrayList<>();
        Iterator<String> iterator = sets.iterator();
        while(iterator.hasNext()){
            setsFromListSets.add(iterator.next());
        }
        // if setsFromListSets is still empty get the sets from ListSet
        // ie; either set is set to ALL or empty
        List<String> setsToBeDeleted= Collections.EMPTY_LIST;
        if (setsFromListSets.isEmpty()) {
            setsToBeDeleted = getSetsToBeDeleted(oaipmhServer);
            setsFromListSets = getSetsFromListSet(oaipmhServer, lastHarvestDate,setsToBeDeleted);
            selectiveUpdate = true;
        }
        LOG.info("{} Sets to be harvested : {} ", setsFromListSets.size(), setsFromListSets);
        initThreadPool(setsFromListSets.size(), selectiveUpdate);
        DownloadsStatus status = new DownloadsStatus(setsFromListSets.size(), 0, new Date(start));

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
            tasks.add(new ListSetsExecutor(setsFromListSets.subList(fromIndex, toIndex), metadataPrefix, directoryLocation, oaipmhServer, logProgressInterval));
        }
        try {
            // invoke a separate thread for each provider
            results = threadPool.invokeAll(tasks);
            List<String> setsDownloaded = new ArrayList<>();
            ListRecordsResult listRecordsResult;
            Map<String,String>  failedrecordPerDownloadedSet = new HashMap<>();

            for (Future<ListRecordsResult> result : results) {
                listRecordsResult = result.get();
                LOG.info("Executor finished with {} errors in {} sec.",
                        listRecordsResult.getErrors(), listRecordsResult.getTime());
                counter += perThread;
                // get the successfully downloaded sets
                if(StringUtils.isNotEmpty(listRecordsResult.getSetsDownloaded())) {
                    setsDownloaded.addAll(Arrays.asList(listRecordsResult.getSetsDownloaded().split("\\s*,\\s*")));
                }
                failedrecordPerDownloadedSet.putAll(listRecordsResult.getFailedRecordCountPerSet());
                logger.logProgress(counter);
            }
            // setsDownloaded only contains successfully downloaded sets
            status.setSetsRecordCountMap(getRecordsCount(setsDownloaded));
            status.setSetsHarvested(setsDownloaded.size());
            getFailedSets(setsFromListSets, setsDownloaded, directoryLocation);
            // After getFailedSets(), setsFromListSets contains failed sets now
            // fail safe check
            // if successful sets + failed sets is not equal to total list sets size, log an error
            failSafeCheck(status.getNoOfSets(), status.getSetsHarvested(), setsFromListSets.size());

            Map<String, ZipFileStatus> fileStatusMap = getFileStatusMap(setsDownloaded, lastHarvestDate,currentHarvestStartTime,
                failedrecordPerDownloadedSet);
            updateFileStatusMapForDeletedSets(fileStatusMap,setsToBeDeleted);
            updateFileStatusMapForTheFailedSets(fileStatusMap,setsFromListSets); // at this point setsFromListSets contains sets which are not downloaded

            status.setsFileStatusMap(fileStatusMap);
            status.setFailedRecordsCountMap(failedrecordPerDownloadedSet);
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
            LOG.info("Last Harvest date set to : "+ (SetsUtility.getLastHarvestDate(directoryLocation + Constants.PATH_SEPERATOR
                    + Constants.HARVEST_DATE_FILENAME, set)).trim());
        }
        clean();
        String timeElapsed = ProgressLogger.getDurationText(System.currentTimeMillis() - start);
        status.setTimeElapsed(timeElapsed);
        LOG.info("ListRecords for all {} sets executed in {}. Harvested {} sets." ,status.getNoOfSets(), timeElapsed, status.getSetsHarvested() );
        return status;
    }

    /*
     This Method updates the status for the sets which were requested but were not downloaded
     */
    private void updateFileStatusMapForTheFailedSets(Map<String, ZipFileStatus> fileStatusMap, List<String> setsFromListSets) {
        for (String failedSetId:setsFromListSets){
          if(fileStatusMap.get(failedSetId)==null) {
              fileStatusMap.put(failedSetId, ZipFileStatus.NA);
          }
        }
    }

    private void updateFileStatusMapForDeletedSets(Map<String, ZipFileStatus> fileStatusMap, List<String> setsDeleted) {
        //Put status for deleted sets
        for (String deletedSetId: setsDeleted){
            fileStatusMap.put(deletedSetId,ZipFileStatus.DELETED);
        }
    }

    /**
     * Returns the Map<String,Long> with sets and the record count
     * get detailed record count for report
     *
     * @param setsDownloaded
     * @return
     */
    private Map<String,Long> getRecordsCount(List<String> setsDownloaded) {
        Map<String,Long> setsRecordsCountMap = new HashMap<>();
        for (String dataset: setsDownloaded) {
            long records = ZipUtility.getNumberOfEntriesInZip(SetsUtility.getFolderName(directoryLocation, Constants.XML_FILE),
                    dataset + Constants.ZIP_EXTENSION);
            setsRecordsCountMap.put(dataset,records);
        }
        return setsRecordsCountMap;
    }

    /**
     * Logs an error if successful sets + failed sets is not equal
     * to total list sets size
     */
    private void failSafeCheck(int totalsSets, int setsHarvested, int failedsets) {
        if(((setsHarvested + failedsets) != totalsSets)) {
            LOG.error("Something went wrong. Sets size mismatch. Total Sets to be harvested {}, " +
                    "Successful harvested sets : {}, Failed Sets : {}", totalsSets, setsHarvested,failedsets);
        }
    }

    /**
     * gets the list of sets to be execeuted download
     * if set == ALL, gets all the sets present
     * if set is Empty and lastHarvestDate is present, gets all the updated, newly created and de-published datasets
     * Also deletes the de-published datasets
     *
     * @return list of  datasets
     */
    private List<String> getSetsFromListSet (OAIPMHServiceClient oaipmhServer, String lastHarvestDate, List<String> setsToBeDeleted ) {
        ListSetsQuery setsQuery = new ListSetsQuery(logProgressInterval);
        List<String> setsFromListSets;
        // if lastHarvestDate is empty, this is the first time we're running the downloads, so get everything
        if (lastHarvestDate.isEmpty()) {
            setsFromListSets = setsQuery.getSets(oaipmhServer, null, null);
            LOG.info("There is no lastHarvestDate. ALL {} sets ready for harvest.", setsFromListSets.size());
        }
        // Check for Updated, newly created and de-published datasets.
        // For de-published dataset XML folder is read
       else {

            // delete the de-published datasets from XML and TTL folders
            if (! setsToBeDeleted.isEmpty()) {
                LOG.info("{} De-published datasets : {} ", setsToBeDeleted.size(), setsToBeDeleted);
                SetsUtility.deleteDataset(setsToBeDeleted, directoryLocation, Constants.XML_FILE);
                SetsUtility.deleteDataset(setsToBeDeleted, directoryLocation, Constants.TTL_FILE);

            } else {
                LOG.info("There are no De-published datasets");
            }
            //get the updated or newly added datasets
            LOG.info("Executing ListSet to get Updated/Newly-Created datasets since {}", lastHarvestDate);
            setsFromListSets = setsQuery.getSets(oaipmhServer, lastHarvestDate, null);
            LOG.info("Updated or newly created datasets count is {} ", setsFromListSets.size());
        }
        return setsFromListSets;
    }

    private List<String> getSetsToBeDeleted(OAIPMHServiceClient oaipmhServer) {
      return SetsUtility.getSetsToBeDeleted(oaipmhServer,
                SetsUtility.getFolderName(directoryLocation, Constants.XML_FILE), logProgressInterval);
    }

    private void executeListRecords(OAIPMHServiceClient oaipmhServer, String setIdentifier) {
        long counter = 0;
        recordsDownloaded =0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger( setIdentifier, -1, logProgressInterval);

        String request = getRequest(oaipmhServer.getOaipmhServer(), setIdentifier);
        ListRecordsResponse response = oaipmhServer.getListRecordRequest(request);
        ListRecords responseObject = response.getListRecords();
        // Create Both zips
        String xmlZipName = SetsUtility.getZipsFolder(directoryLocation, Constants.XML_FILE, setIdentifier);
        String ttlZipName = SetsUtility.getZipsFolder(directoryLocation, Constants.TTL_FILE, setIdentifier);

            try (final ZipOutputStream xmlZout = new ZipOutputStream(
                new FileOutputStream(new File(xmlZipName)));
                final ZipOutputStream ttlZout = new ZipOutputStream(
                    new FileOutputStream(new File(ttlZipName)));
                OutputStreamWriter writer = new OutputStreamWriter(xmlZout);
                OutputStreamWriter writer1 = new OutputStreamWriter(ttlZout)) {

                //writing in ZIP

                if (responseObject != null) {
                    counter += responseObject.getRecords().size();

                    if (responseObject.getResumptionToken() != null) {
                        logger.setTotalItems(
                            responseObject.getResumptionToken().getCompleteListSize());

                        recordsTobeDownloaded = String.valueOf(responseObject.getResumptionToken().getCompleteListSize());
                    } else {
                        logger.setTotalItems(responseObject.getRecords().size());
                        recordsTobeDownloaded = String.valueOf(responseObject.getRecords().size());

                    }

                    for (Record record : responseObject.getRecords()) {
                        ZipUtility.writeInZip(xmlZout, writer, record, Constants.XML_FILE);
                        ZipUtility.writeInZip(ttlZout, writer1, record, Constants.TTL_FILE);
                        recordsDownloaded++;
                    }

                    while (responseObject.getResumptionToken() != null) {

                        request = getResumptionRequest(oaipmhServer.getOaipmhServer(),
                            responseObject.getResumptionToken().getValue());
                        response = oaipmhServer.getListRecordRequest(request);
                        responseObject = response.getListRecords();
                        //writing in ZIP
                        for (Record record : responseObject.getRecords()) {
                            ZipUtility.writeInZip(xmlZout, writer, record, Constants.XML_FILE);
                            ZipUtility.writeInZip(ttlZout, writer1, record, Constants.TTL_FILE);
                            recordsDownloaded++;
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
            finally {
                LOG.info("Dataset:"+set+ " Total records: "+recordsTobeDownloaded + " Downloaded:" + recordsDownloaded + " Failed records:" +(Long.valueOf(recordsTobeDownloaded)-recordsDownloaded));
            }
            //create MD5Sum file for the XML and TTL zip
            ZipUtility.createMD5SumFile(xmlZipName);
            ZipUtility.createMD5SumFile(ttlZipName);

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
        // log the new failed set file values
        LOG.info("Failed sets file values : " + CSVFile.readCSVFile(CSVFile.getCsvFilePath(directoryLocation)));
    }

    private Map<String,ZipFileStatus> getFileStatusMap(List<String> setsDownloaded,String lastHarvestDate,
        Date currentHarvestStartTime, Map<String, String> failedrecordPerSet) {
        Map<String,ZipFileStatus> statusMap = new HashMap<>();
        //Put status for New or Unchange or Changed or Reharvested sets
        for(String setId : setsDownloaded){
            String fileName = SetsUtility.getFolderName(directoryLocation, Constants.XML_FILE)+Constants.PATH_SEPERATOR+setId + Constants.ZIP_EXTENSION;
            statusMap.put(setId,ZipUtility.generateFileStatus(fileName,lastHarvestDate,currentHarvestStartTime));
        }
        //Put status for failed sets , these are considered as the partially downloaded sets as we do not have all records properly downloaded
        for(Entry<String,String> kv : failedrecordPerSet.entrySet()){
            if(statusMap.get(kv.getKey())==null) {
                String fileName = SetsUtility.getFolderName(directoryLocation, Constants.XML_FILE)
                    + Constants.PATH_SEPERATOR + kv.getKey() + Constants.ZIP_EXTENSION;
                statusMap.put(kv.getKey(), ZipUtility.generateFileStatus(fileName, lastHarvestDate,
                    currentHarvestStartTime));
            }
        }
        return statusMap;
    }

    @PreDestroy
    private void clean() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public String getDownloadServerURL() {
        return downloadServerURL;
    }
}

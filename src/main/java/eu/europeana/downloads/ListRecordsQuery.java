package eu.europeana.downloads;

import eu.europeana.oaipmh.model.ListRecords;
import eu.europeana.oaipmh.model.Record;
import eu.europeana.oaipmh.model.response.ListRecordsResponse;
import eu.europeana.oaipmh.service.exception.OaiPmhException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private List<String> sets = new ArrayList<>();

    private ExecutorService threadPool;

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
    public void execute(OAIPMHServiceClient oaipmhServer) throws OaiPmhException {
        if (sets.size() != 1 && threads > 1) {
            executeMultithreadListRecords(oaipmhServer, sets);
        } else {
            executeListRecords(oaipmhServer, set, fileFormat);
        }
    }

    private void executeMultithreadListRecords(OAIPMHServiceClient oaipmhServer, List<String> sets) {
        initThreadPool();
        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger("Multiple sets", -1, logProgressInterval);
        List<String> setsFromListSets = sets;
        if (sets.isEmpty()) {
            ListSetsQuery setsQuery = new ListSetsQuery(logProgressInterval);
            setsFromListSets = setsQuery.getSets(oaipmhServer);
        }
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

            ListRecordsResult listRecordsResult;
            for (Future<ListRecordsResult> result : results) {
                listRecordsResult = result.get();
                LOG.info("Executor finished with {} errors in {} sec.",
                        listRecordsResult.getErrors(), listRecordsResult.getTime());
                counter += perThread;
                logger.logProgress(counter);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted.", e);
        } catch (ExecutionException e) {
            LOG.error("Problem with task thread execution.", e);
        }

        clean();

        LOG.info("ListRecords for all sets executed in " + ProgressLogger.getDurationText(System.currentTimeMillis() - start) +
                ". Harvested " + sets.size() + " sets.");
    }

    private void executeListRecords(OAIPMHServiceClient oaipmhServer, String setIdentifier, String fileFormat) {
        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger( setIdentifier, -1, logProgressInterval);

        String request = getRequest(oaipmhServer.getOaipmhServer(), setIdentifier);
        ListRecordsResponse response = oaipmhServer.getListRecordRequest(request);
        ListRecords responseObject = response.getListRecords();
        String zipName = directoryLocation + Constants.PATH_SEPERATOR + setIdentifier + Constants.ZIP_EXTENSION ;
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
        //create CRC file for the Zip
        ZipUtility.createCRCFile(zipName);

        LOG.info("ListRecords for set " + setIdentifier + " executed in " + ProgressLogger.getDurationText(System.currentTimeMillis() - start) +
                ". Harvested " + counter + " records.");
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

    @PreDestroy
    private void clean() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }
}

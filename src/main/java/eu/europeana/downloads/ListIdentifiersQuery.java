package eu.europeana.downloads;

import eu.europeana.oaipmh.model.Header;
import eu.europeana.oaipmh.model.ListIdentifiers;
import eu.europeana.oaipmh.model.response.ListIdentifiersResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;

/**
 * @deprecated  This is an alternative approach. Not used currently
 */
@Deprecated (since = "15-July-2020")
@Component
public class ListIdentifiersQuery extends BaseQuery implements OAIPMHQuery {

    private static final Logger LOG = LogManager.getLogger(ListIdentifiersQuery.class);

    @Value("${metadata-prefix}")
    private String metadataPrefix;

    @Value("${harvest-from}")
    private String from;

    @Value("${harvest-sets}")
    private String set;

    @Value("${log-progress-interval}")
    private Integer logProgressInterval;

    @Value("${harvest-threads}")
    private int threads;

    @Value("${sets-folder}")
    private String directoryLocation;

    @Value("${file-format}")
    private String fileFormat;

    private ExecutorService threadPool;

    private List<String> sets = new ArrayList<>();

    public ListIdentifiersQuery() {
    }

    public ListIdentifiersQuery(String metadataPrefix, String from, String set, int logProgressInterval) {
        this.metadataPrefix = metadataPrefix;
        this.from = from;
        this.set = set;
        this.logProgressInterval = logProgressInterval;
    }

    @PostConstruct
    public void initSets() {
        if (set != null && !set.isEmpty() && !StringUtils.equals(set, "ALL")) {
            sets.addAll(Arrays.asList(set.split(",")));
        }
    }

    private void initThreadPool() {
        // init thread pool
        if (threads < 1 || sets.size() == 1) {
            threads = 1;
        }
        threadPool = Executors
                .newFixedThreadPool(threads);
    }
    @Override
    public String getVerbName() {
        return Constants.LIST_IDENTIFIERS_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer, List<String> sets) {
        if (sets.size() != 1 && threads > 1) {
                executeMultithreadListRecords(oaipmhServer, sets);
            }
         else {
            executeSingleThreadListRecord(oaipmhServer, set);
        }
    }

    public List<String> getIdentifiers(OAIPMHServiceClient oaipmhServer, String setName) {
        List<String> identifiers = new ArrayList<>();
        execute(oaipmhServer, setName, identifiers);
        return identifiers;
    }

    private void executeSingleThreadListRecord(OAIPMHServiceClient oaipmhServer, String setIdentifier) {
        initThreadPool();
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger("Single set", 1, logProgressInterval);

        LOG.info(" {} Set to be executed by {} threads", setIdentifier, threads);

        List<Future<ListRecordsResult>> results = null;
        List<Callable<ListRecordsResult>> tasks = new ArrayList<>();
        tasks.add(new ListIdentifierExecutor(sets, from, metadataPrefix, directoryLocation, oaipmhServer, logProgressInterval));
        try {
            // invoke a separate thread for each provider
            results = threadPool.invokeAll(tasks);

            ListRecordsResult listRecordsResult;
            for (Future<ListRecordsResult> result : results) {
                listRecordsResult = result.get();
                LOG.info("Executor finished with {} errors in {} sec.",
                        listRecordsResult.getErrors(), listRecordsResult.getTime());
                logger.logProgress(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted.", e);
        } catch (ExecutionException e) {
            LOG.error("Problem with task thread execution.", e);
        }

        clean();
        LOG.info("ListIdentifier for set {} executed in {}. Harvested {} sets. ",setIdentifier,
                ProgressLogger.getDurationText(System.currentTimeMillis() - start), sets.size());
    }

    private void executeMultithreadListRecords(OAIPMHServiceClient oaipmhServer, List<String> sets) {
        initThreadPool();
        List<String> setsFromListSets = sets;

        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger("Multiple sets", -1, logProgressInterval);

        // get all the sets in a list
        if (sets.isEmpty()) {
            ListSetsQuery setsQuery = new ListSetsQuery(logProgressInterval);
            setsFromListSets = setsQuery.getSets(oaipmhServer, null, null);
        }
        LOG.info(" {} Sets to be executed by {} threads", setsFromListSets.size(), threads);
        List<Future<ListRecordsResult>> results = null;
        List<Callable<ListRecordsResult>> tasks = new ArrayList<>();

        int perThread = setsFromListSets.size() / threads;

        for (int i = 0; i < threads; i++) {
            int fromIndex = i * perThread;
            int toIndex = (i + 1) * perThread;
            if (i == threads - 1) {
                toIndex = setsFromListSets.size();
            }
            tasks.add(new ListIdentifierExecutor(setsFromListSets.subList(fromIndex, toIndex), from, metadataPrefix, directoryLocation, oaipmhServer, logProgressInterval));
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

        LOG.info("ListIdentifier for all sets executed in {}. Harvested {} sets ", ProgressLogger.getDurationText(System.currentTimeMillis() - start), sets.size());
    }

    private void execute(OAIPMHServiceClient oaipmhServer, String setName, List<String> identifiers) {
        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger(setName,-1, logProgressInterval);

        String request = getRequest(oaipmhServer.getOaipmhServer(), setName);

        ListIdentifiersResponse response = (ListIdentifiersResponse) oaipmhServer.makeRequest(request, ListIdentifiersResponse.class);
        ListIdentifiers responseObject = response.getListIdentifiers();
        if (responseObject != null) {
            counter += responseObject.getHeaders().size();
            if (responseObject.getResumptionToken() != null) {
                logger.setTotalItems(responseObject.getResumptionToken().getCompleteListSize());
            } else {
                logger.setTotalItems(responseObject.getHeaders().size());
            }
            collectIdentifiers(responseObject.getHeaders(), identifiers);
            //writeDataToLogFile(responseObject);

            while (responseObject.getResumptionToken() != null) {
                request = getResumptionRequest(oaipmhServer.getOaipmhServer(), responseObject.getResumptionToken().getValue());
                response = (ListIdentifiersResponse) oaipmhServer.makeRequest(request, ListIdentifiersResponse.class);
                responseObject = response.getListIdentifiers();
                if (responseObject == null) {
                    break;
                }
                counter += responseObject.getHeaders().size();
                logger.logProgress(counter);
                collectIdentifiers(responseObject.getHeaders(), identifiers);
              //  writeDataToLogFile(responseObject);
            }
        }

        LOG.info("ListIdentifiers for set {} executed in {}. Retrieved {} identifiers", setName,
                ProgressLogger.getDurationText(System.currentTimeMillis() - start) ,counter);
    }

    /**
     * Temporarily added for testing/debugging purposes
     */
    private void writeDataToLogFile(ListIdentifiers responseObject) {
        // write data to file
        ListIterator<Header> it = responseObject.getHeaders().listIterator();
        while (it.hasNext()) {
            Header header = it.next();
            LogFile.OUT.info("{} {}", header.getSetSpec().get(0), header.getIdentifier());
        }
    }

    private void collectIdentifiers(List<Header> headers, List<String> identifiers) {
        if (identifiers != null) {
            for (Header header : headers) {
                identifiers.add(header.getIdentifier());
            }
        }
    }

    private String getResumptionRequest(String oaipmhServer, String resumptionToken) {
        return getBaseRequest(oaipmhServer, getVerbName()) +
                String.format(RESUMPTION_TOKEN_PARAMETER, resumptionToken);
    }

    private String getRequest(String oaipmhServer, String set) {
        StringBuilder sb = new StringBuilder();
        sb.append(getBaseRequest(oaipmhServer, getVerbName()));
        sb.append(String.format(METADATA_PREFIX_PARAMETER, metadataPrefix));
        if (from != null && !from.isEmpty()) {
            sb.append(String.format(FROM_PARAMETER, from));
        }
        if (set != null) {
            sb.append(String.format(SET_PARAMETER, set));
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

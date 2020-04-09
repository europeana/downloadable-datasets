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
        if (threads < 1) {
            threads = 1;
        }
        threadPool = Executors
                .newFixedThreadPool(threads);
    }
    @Override
    public String getVerbName() {
        return "ListIdentifiers";
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer) {
        if (sets.size() != 1 && threads > 1) {
            if (sets.isEmpty())  {
                executeMultithreadListRecords(oaipmhServer, null);
             } else {
                for (String setIdentifier : sets) {
                    executeMultithreadListRecords(oaipmhServer, setIdentifier);
                }
            }
        } else {
            executeSingleThreadListRecord(oaipmhServer, set);
        }
    }

    public List<String> getIdentifiers(OAIPMHServiceClient oaipmhServer, String setName) {
        List<String> identifiers = new ArrayList<>();
        execute(oaipmhServer, setName, identifiers);
        return identifiers;
    }

    private void executeSingleThreadListRecord(OAIPMHServiceClient oaipmhServer, String setIdentifier) {
        List<String> identifiers = getIdentifiers(oaipmhServer, setIdentifier);
        for(String identifier : identifiers) {
            new GetRecordQuery(metadataPrefix, identifier, directoryLocation).execute(oaipmhServer);
        }
    }

    private void executeMultithreadListRecords(OAIPMHServiceClient oaipmhServer, String setIdentifier) {
        initThreadPool();

        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger(setIdentifier, -1, logProgressInterval);

        ListIdentifiersQuery identifiersQuery = prepareListIdentifiersQuery(setIdentifier);
        List<String> identifiers = identifiersQuery.getIdentifiers(oaipmhServer, setIdentifier);
        logger.setTotalItems(identifiers.size());

        List<Future<ListRecordsResult>> results = null;
        List<Callable<ListRecordsResult>> tasks = new ArrayList<>();

        int perThread = identifiers.size() / threads;

        // create task for each resource provider
        for (int i = 0; i < threads; i++) {
            int fromIndex = i * perThread;
            int toIndex = (i + 1) * perThread;
            if (i == threads - 1) {
                toIndex = identifiers.size();
            }
            tasks.add(new ListRecordsExecutor(setIdentifier, identifiers.subList(fromIndex, toIndex), metadataPrefix, directoryLocation, oaipmhServer, logProgressInterval));
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

        LOG.info("ListRecords for set " + setIdentifier + " executed in " + ProgressLogger.getDurationText(System.currentTimeMillis() - start) +
                ". Harvested " + identifiers.size() + " identifiers.");
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

        LOG.info("ListIdentifiers for set " + setName + " executed in " + ProgressLogger.getDurationText(System.currentTimeMillis() - start) +
                ". Harvested " + counter + " identifiers.");
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

    private ListIdentifiersQuery prepareListIdentifiersQuery(String setIdentifier) {
        ListIdentifiersQuery query = new ListIdentifiersQuery(metadataPrefix, from, setIdentifier, 30);
        query.initSets();
        return query;
    }

    @PreDestroy
    private void clean() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }
}

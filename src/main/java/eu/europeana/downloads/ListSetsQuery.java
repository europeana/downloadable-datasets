package eu.europeana.downloads;

import eu.europeana.oaipmh.model.ListSets;
import eu.europeana.oaipmh.model.Set;
import eu.europeana.oaipmh.model.response.ListSetsResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ListSetsQuery extends BaseQuery implements OAIPMHQuery  {
    private static final Logger LOG = LogManager.getLogger(ListSetsQuery.class);

    @Value("${log-progress-interval}")
    private Integer logProgressInterval;

    @Value("${metadata-prefix}")
    private String metadataPrefix;

    public ListSetsQuery() {
    }

    public ListSetsQuery(int logProgressInterval) {
        this.logProgressInterval = logProgressInterval;
    }

    @Override
    public String getVerbName(){ return Constants.LIST_SET_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer) {
        execute(oaipmhServer, null);
    }

    public List<String> getSets(OAIPMHServiceClient oaipmhServer) {
        List<String> setsFromListSets = new ArrayList<>();
        execute(oaipmhServer, setsFromListSets );
        return setsFromListSets;
    }

    private void execute(OAIPMHServiceClient oaipmhServer, List<String> setsFromListSet) {
        long counter = 0;
        long start = System.currentTimeMillis();
        ProgressLogger logger = new ProgressLogger("All sets", -1, logProgressInterval);

        String request = getRequest(oaipmhServer.getOaipmhServer());

        ListSetsResponse response = (ListSetsResponse) oaipmhServer.makeRequest(request, ListSetsResponse.class);
        ListSets responseObject = response.getListSets();

        if (responseObject != null) {
            counter += responseObject.getSets().size();
            if (responseObject.getResumptionToken() != null) {
                logger.setTotalItems(responseObject.getResumptionToken().getCompleteListSize());
            } else {
                logger.setTotalItems(responseObject.getSets().size());
            }
             collectSets(responseObject.getSets(), setsFromListSet);

            while (responseObject.getResumptionToken() != null) {
                request = getResumptionRequest(oaipmhServer.getOaipmhServer(), responseObject.getResumptionToken().getValue());
                response = (ListSetsResponse) oaipmhServer.makeRequest(request, ListSetsResponse.class);
                responseObject = response.getListSets();
                if (responseObject == null) {
                    break;
                }
                counter += responseObject.getSets().size();
                logger.logProgress(counter);
                collectSets(responseObject.getSets(), setsFromListSet);
            }
        }

        LOG.info("ListSet  executed in " + ProgressLogger.getDurationText(System.currentTimeMillis() - start) +
                ". Harvested " + counter + " sets.");
    }

    private void collectSets(List<Set> sets , List<String> setsFromListSet) {
        if (setsFromListSet != null) {
            for (Set set : sets) {
                setsFromListSet.add(set.getSetSpec());
            }
        }
    }

    private String getResumptionRequest(String oaipmhServer, String resumptionToken) {
        return getBaseRequest(oaipmhServer, getVerbName()) +
                String.format(RESUMPTION_TOKEN_PARAMETER, resumptionToken);
    }

    private String getRequest(String oaipmhServer) {
        StringBuilder sb = new StringBuilder();
        sb.append(getBaseRequest(oaipmhServer, getVerbName()));
        return sb.toString();
    }
}

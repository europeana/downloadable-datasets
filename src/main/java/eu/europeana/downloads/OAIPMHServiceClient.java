package eu.europeana.downloads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import eu.europeana.oaipmh.model.RDFMetadata;
import eu.europeana.oaipmh.model.response.*;
import eu.europeana.oaipmh.model.serialize.*;
import eu.europeana.oaipmh.service.exception.OaiPmhException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.XML;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OAIPMHServiceClient {

    private static final Logger LOG = LogManager.getLogger(OAIPMHServiceClient.class);

    @Value("${oaipmh-server}")
    private String oaipmhServer;

    @Value("${harvest-method}")
    private String harvestMethod;

    @Value("${sets-folder}")
    private String directoryLocation;

    private RestTemplate restTemplate = new RestTemplate();

    private ObjectMapper mapper;

    @Autowired
    private ListIdentifiersQuery listIdentifiersQuery;

    @Autowired
    private ListRecordsQuery listRecordsQuery;

    @Autowired
    private ListSetsQuery listSetsQuery;

    @Autowired
    private CheckSumGenerator checkSumGenerator;

    private Map<String, OAIPMHQuery> queries = new HashMap<>();

    @PostConstruct
    public void init() {
        queries.put("ListIdentifiers", listIdentifiersQuery);
        queries.put("ListRecords", listRecordsQuery);
        queries.put("ListSets", listSetsQuery);
        queries.put("CheckSum", checkSumGenerator);

        mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ListIdentifiersResponse.class, new ListIdentifiersResponseDeserializer());
        module.addDeserializer(GetRecordResponse.class, new GetRecordResponseDeserializer());
        module.addDeserializer(RDFMetadata.class, new RDFMetadataDeserializer());
        module.addDeserializer(ListRecordsResponse.class, new ListRecordsResponseDeserializer());
        module.addDeserializer(ListSetsResponse.class, new ListSetsResponseDeserializer());
        mapper.registerModule(module);
        LOG.info("Using OAI-PMH server at {}", oaipmhServer);
    }

    public String getOaipmhServer() {
        return oaipmhServer;
    }

    /**
     * Will execute the verb and retry the Failed sets.
     * Failed sets will be retried if any exist from previous run
     * If there are no failed sets, the retry mechanism will not be executed.
     *
     * @param verb
     * @throws OaiPmhException
     */
    public void execute(String verb) throws OaiPmhException {
        OAIPMHQuery verbToExecute = queries.get(verb);

        if (verbToExecute == null) {
            return;
        }
        SetsUtility.createFolders(directoryLocation);

        // failed sets will not be retied in verb is Checksum
        if (!StringUtils.equals(Constants.CHECKSUM_VERB, verbToExecute.getVerbName())) {
            // First check for failed sets from previous run
            List<String> failedSets = CSVFile.readCSVFile(CSVFile.getCsvFilePath(directoryLocation));
            if (!failedSets.isEmpty()) {
                LOG.info("Found {} failed sets from previous run - {}", failedSets.size(), failedSets);
                verbToExecute.execute(this, failedSets);
            } else {
                LOG.info("No failed sets exist for this harvest.");
            }
        }
        verbToExecute.execute(this, null);
    }

    public OAIResponse makeRequest(String request, Class<? extends OAIResponse> responseClass) {
        OAIResponse response = null;
        String responseAsString = restTemplate.getForObject(request, String.class);
        String json = XML.toJSONObject(responseAsString).toString();
        try {
            response = mapper.readValue(json, responseClass);
        } catch (IOException e) {
            LOG.error("Exception when deserializing response.", e);
        }
        return response;
    }


    public String getHarvestMethod() {
        return harvestMethod;
    }

    public GetRecordResponse getGetRecordRequest(String request) {
        String responseAsString = restTemplate.getForObject(request, String.class);
        return XMLResponseParser.parseGetRecordResponse(responseAsString);
    }

    public ListRecordsResponse getListRecordRequest(String request) {
        String responseAsString = restTemplate.getForObject(request, String.class);
        return XMLResponseParser.parseListRecordResponse(responseAsString);
    }
}

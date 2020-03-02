package eu.europeana.downloads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import eu.europeana.oaipmh.model.RDFMetadata;
import eu.europeana.oaipmh.model.response.*;
import eu.europeana.oaipmh.model.serialize.*;
import eu.europeana.oaipmh.service.exception.OaiPmhException;
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
import java.util.Map;

@Service
public class OAIPMHServiceClient {

    private static final Logger LOG = LogManager.getLogger(OAIPMHServiceClient.class);

    @Value("${oaipmh-server}")
    private String oaipmhServer;

    private RestTemplate restTemplate = new RestTemplate();

    private ObjectMapper mapper;

    @Autowired
    private ListIdentifiersQuery listIdentifiersQuery;

    @Autowired
    private GetRecordQuery getRecordQuery;

    @Autowired
    private ListRecordsQuery listRecordsQuery;

    @Autowired
    private ListSetsQuery listSetsQuery;

    private Map<String, OAIPMHQuery> queries = new HashMap<>();

    @PostConstruct
    public void init() {
        queries.put("ListIdentifiers", listIdentifiersQuery);
        queries.put("ListRecords", listRecordsQuery);
        queries.put("ListSets", listSetsQuery);

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

    public void execute(String verb) throws OaiPmhException {
        OAIPMHQuery verbToExecute = queries.get(verb);
        if (verbToExecute != null) {
            //LogFile.setFileName(verbToExecute.getVerbName());
            verbToExecute.execute(this);
        }
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
}

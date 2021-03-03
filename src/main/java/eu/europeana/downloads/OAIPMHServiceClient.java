package eu.europeana.downloads;

import com.ctc.wstx.util.StringUtil;
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
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

        if(verbToExecute == null){
            return;
        }
        SetsUtility.createFolders(directoryLocation);

        // temp code
        if(StringUtils.equals(Constants.CHECKSUM_VERB, verbToExecute.getVerbName())) {
            // delete failed sets file
            File xmlfile = new File(SetsUtility.getFolderName(directoryLocation, Constants.XML_FILE)
                    + Constants.PATH_SEPERATOR + Constants.CSV_FILE + Constants.CSV_EXTENSION);
            File ttlfile = new File(SetsUtility.getFolderName(directoryLocation, Constants.TTL_FILE)
                    + Constants.PATH_SEPERATOR + Constants.CSV_FILE + Constants.CSV_EXTENSION);
            //delete lastharvest date file
            File xmlfile1 = new File(SetsUtility.getFolderName(directoryLocation, Constants.XML_FILE)
                    + Constants.PATH_SEPERATOR + Constants.HARVEST_DATE_FILENAME);

            File ttlfile1 = new File(SetsUtility.getFolderName(directoryLocation, Constants.TTL_FILE)
                    + Constants.PATH_SEPERATOR + Constants.HARVEST_DATE_FILENAME);
           LOG.info("removing : {} , {} , {}, {}",  xmlfile, ttlfile, xmlfile1, ttlfile1);

            try {
                Files.deleteIfExists(xmlfile.toPath());
                Files.deleteIfExists(ttlfile.toPath());
                Files.deleteIfExists(xmlfile1.toPath());
                Files.deleteIfExists(ttlfile1.toPath());

            } catch (IOException e) {
                LOG.info("error removing : {} , {} , {}, {}",  xmlfile, ttlfile, xmlfile1, ttlfile1);
            }

            // create a harvest date file
            LOG.info("Creating/Updating the {} file ", Constants.HARVEST_DATE_FILENAME);
            try (PrintWriter writer = new PrintWriter(directoryLocation + Constants.PATH_SEPERATOR + Constants.HARVEST_DATE_FILENAME, StandardCharsets.UTF_8)) {
                DateFormat formatter = new SimpleDateFormat(Constants.HARVEST_DATE_FORMAT);
                writer.write("2021-02-23T00:00:20Z");
            } catch (IOException e) {
                LOG.error("Error writing the {} file ", Constants.HARVEST_DATE_FILENAME, e);
            }

        } else {
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

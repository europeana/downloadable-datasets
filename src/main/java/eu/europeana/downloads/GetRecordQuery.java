package eu.europeana.downloads;

import eu.europeana.oaipmh.model.GetRecord;
import eu.europeana.oaipmh.model.Header;
import eu.europeana.oaipmh.model.RDFMetadata;
import eu.europeana.oaipmh.model.Record;
import eu.europeana.oaipmh.model.response.GetRecordResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import java.io.OutputStreamWriter;
import java.util.zip.ZipOutputStream;

/**
 * @deprecated  This is an alternative approach. Not used currently
 */
@Deprecated (since = "15-July-2020")
@Component
public class GetRecordQuery extends BaseQuery implements OAIPMHQuery {

    private static final Logger LOG = LogManager.getLogger(GetRecordQuery.class);

    private String metadataPrefix;

    private String fileFormat;

    private String identifier;

    private ZipOutputStream zipOutputStream;

    private OutputStreamWriter writer;

    public GetRecordQuery() {
    }

    public GetRecordQuery(String metadataPrefix, String identifier, ZipOutputStream zipOutputStream, OutputStreamWriter writer) {
        this.metadataPrefix = metadataPrefix;
        this.identifier = identifier;
        this.zipOutputStream = zipOutputStream;
        this.writer = writer;
    }

    @Override
    public String getVerbName() {
        return Constants.GET_RECORD_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer) {
        execute(oaipmhServer, identifier, zipOutputStream,writer);
    }

    private void execute(OAIPMHServiceClient oaipmhServer, String currentIdentifier, ZipOutputStream zout, OutputStreamWriter writer) {
        long start = System.currentTimeMillis();

        String request = getRequest(oaipmhServer.getOaipmhServer(), currentIdentifier);
        GetRecordResponse response = oaipmhServer.getGetRecordRequest(request);
        GetRecord responseObject = response.getGetRecord();
        if (responseObject != null) {
            Record recordVal = responseObject.getRecord();
            if (recordVal == null) {
                LOG.error("No record in GetRecordResponse for identifier {}", currentIdentifier);
                return;
            }
            Header header = recordVal.getHeader();
            if (header != null && currentIdentifier.equals(header.getIdentifier())) {
                RDFMetadata metadata = recordVal.getMetadata();
                if (metadata == null || metadata.getMetadata() == null || metadata.getMetadata().isEmpty()) {
                    LOG.error("Empty metadata for identifier {}", currentIdentifier);
                }
            }
            //write in Zip
            ZipUtility.writeInZip(zout, writer, recordVal, fileFormat);
        }

        LOG.info("GetRecord for identifier {} executed in {} ms", currentIdentifier, (System.currentTimeMillis() - start));
    }

    private String getRequest(String oaipmhServer, String identifier) {
        StringBuilder sb = new StringBuilder();
        sb.append(getBaseRequest(oaipmhServer, getVerbName()));
        sb.append(String.format(METADATA_PREFIX_PARAMETER, metadataPrefix));
        if (identifier != null && !identifier.isEmpty()) {
            sb.append(String.format(IDENTIFIER_PARAMETER, identifier));
        }
        return sb.toString();
    }
}

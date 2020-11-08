package eu.europeana.downloads;

import eu.europeana.oaipmh.service.exception.OaiPmhException;

import java.util.List;

public interface OAIPMHQuery {

    String VERB_PARAMETER = "verb=%s";

    String getVerbName();

    void execute(OAIPMHServiceClient oaipmhServer, List<String> failedSets) throws OaiPmhException;
}

package eu.europeana.downloads;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class CheckSumGenerator extends BaseQuery implements OAIPMHQuery {

    private static final Logger LOG = LogManager.getLogger(CheckSumGenerator.class);

    @Value("${sets-folder}")
    private String directoryLocation;

    @Value("${file-format}")
    private String fileFormat;

    @Override
    public String getVerbName() {
        return Constants.CHECKSUM_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer, List<String> failedSets) {
        LOG.info("Generating CheckSum for the {} files ", fileFormat.isEmpty() ? Constants.XML_FILE : Constants.TTL_FILE);
        generateCheckSum(SetsUtility.getFolderName(directoryLocation, fileFormat));
    }

    /**
     * generates checksum for the zips files present in the directory
     *
     * @param path directory location
     * @return list of zip files
     */
    private void generateCheckSum(String path) {
        List<String> zips = getZipFiles(path);
        for(String zipFile : zips) {
            ZipUtility.createMD5SumFile(path + Constants.PATH_SEPERATOR + zipFile);
        }
        LOG.info("Generated CheckSum for {} files ", zips.size());
    }

    /**
     * gets the zips files from the directory
     *
     * @param location location of the directory
     * @return list of zip files
     */
    private List<String> getZipFiles(String location) {
        return Stream.of(new File(location).listFiles())
                    .filter(file -> file.getName().endsWith(Constants.ZIP_EXTENSION))
                    .map(File::getName)
                    .collect(Collectors.toList());
    }
}

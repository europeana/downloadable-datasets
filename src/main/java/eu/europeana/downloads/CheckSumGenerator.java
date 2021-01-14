package eu.europeana.downloads;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class CheckSumGenerator extends BaseQuery implements OAIPMHQuery {

    private static final Logger LOG = LogManager.getLogger(CheckSumGenerator.class);

    @Value("${sets-folder}")
    private String directoryLocation;

    @Value("#{'${zips-folder:${sets-folder:}}'}")
    private String zipsLocation;

    @Value("${file-format}")
    private String fileFormat;

    @Override
    public String getVerbName() {
        return Constants.CHECKSUM_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer, List<String> failedSets) {
        LOG.info("Generating CheckSum for the {} files ", fileFormat.isEmpty() ? Constants.XML_FILE : fileFormat);
        zipsLocation = zipsLocation.isEmpty() ? directoryLocation : zipsLocation ;
        generateCheckSum(SetsUtility.getFolderName(zipsLocation, fileFormat),SetsUtility.getFolderName(directoryLocation, fileFormat));
    }

    /**
     * generates checksum for the zips files present in the directory
     *
     * @param zipsPath directory location for the zips file
     * @param checkSumPath directory location for where the checksum files will be generated
     */
    private void generateCheckSum(String zipsPath, String checkSumPath) {
        List<String> zips = getZipFiles(zipsPath);
        if (zips.isEmpty()) {
            LOG.info("No zips are present at location \"{}\" OR the \"{}\" directory does not exist.", zipsPath, zipsPath);
            LOG.info(" NOT Generating CheckSum");
        } else {
            for (String zipFile : zips) {
                String extension = Constants.PATH_SEPERATOR + zipFile;
                ZipUtility.createMD5SumFile(zipsPath + extension, checkSumPath + extension);
            }
            LOG.info("Generated CheckSum for {} files ", zips.size());
        }
    }

    /**
     * gets the zips files from the directory
     *
     * @param location location of the directory
     * @return list of zip files
     */
    private List<String> getZipFiles(String location) {
        if (Files.exists(Paths.get(location))) {
            return Stream.of(new File(location).listFiles())
                    .filter(file -> file.getName().endsWith(Constants.ZIP_EXTENSION))
                    .map(File::getName)
                    .collect(Collectors.toList());
        }
        LOG.error("NO such directory exists {}", location);
        return new ArrayList<>();
    }
}

package eu.europeana.downloads;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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

    @Override
    public String getVerbName() {
        return Constants.CHECKSUM_VERB;
    }

    @Override
    public void execute(OAIPMHServiceClient oaipmhServer, List<String> failedSets) {
        zipsLocation = zipsLocation.isEmpty() ? directoryLocation : zipsLocation;
        LOG.info("Generating CheckSum for the XML files ");
        generateCheckSum(SetsUtility.getFolderName(zipsLocation, Constants.XML_FILE), SetsUtility.getFolderName(directoryLocation, Constants.XML_FILE));
        LOG.info("Generating CheckSum for the TTL files ");
        generateCheckSum(SetsUtility.getFolderName(zipsLocation, Constants.TTL_FILE), SetsUtility.getFolderName(directoryLocation, Constants.TTL_FILE));
    }

    /**
     * generates checksum for the zips files present in the directory
     *
     * @param zipsPath     directory location for the zips file
     * @param checkSumPath directory location for where the checksum files will be generated
     */
    private void generateCheckSum(String zipsPath, String checkSumPath) {
        List<String> zips = getZipFilesWithoutCheckSum(zipsPath);
        if (zips.isEmpty()) {
            LOG.info("No zips are present at location \"{}\" OR the \"{}\" directory does not exist.", zipsPath, zipsPath);
            LOG.info(" NO CheckSum are Generated");
        } else {
            LOG.info("Missing Checksum for Zips {} ", zips);
            for (String zipFile : zips) {
                LOG.info("Generating CheckSum for zip {} ", zipFile);
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

    /**
     * gets the zips files from the directory
     *
     * @param location location of the directory
     * @return list of zip files
     */
    private List<String> getZipFilesWithoutCheckSum(String location) {
        if (Files.exists(Paths.get(location))) {
            List<String> zips = new ArrayList<>();
            List<File> files = Arrays.asList(new File(location).listFiles());
            for (File file : files) {
                if (file.getName().endsWith(Constants.ZIP_EXTENSION) && !checksumExists(files, file.getName())) {
                    zips.add(file.getName());
                }
            }
            return zips;
        }
        LOG.error("NO such directory exists {}", location);
        return new ArrayList<>();
    }

    private boolean checksumExists(List<File> fileList, String fileName) {
        for (File checksum : fileList) {
            if (checksum.getName().endsWith(Constants.MD5_EXTENSION) &&
                    StringUtils.equalsIgnoreCase(checksum.getName(), fileName + Constants.MD5_EXTENSION)) {
                return true;
            }
        }
        return false;
    }
}

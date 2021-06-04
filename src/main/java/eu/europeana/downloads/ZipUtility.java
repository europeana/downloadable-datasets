package eu.europeana.downloads;

import eu.europeana.oaipmh.model.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ZipUtility {

    private static final Logger LOG = LogManager.getLogger(ZipUtility.class);

    private ZipUtility() {
        //adding a private constructor to hide implicit public one
    }

    /**
     * method to write in the zip
     */
    public static void writeInZip(ZipOutputStream zout, OutputStreamWriter writer, Record record, String fileFormat) {
        try {
            zout.putNextEntry(new ZipEntry(getEntryName(record, fileFormat)));
            writer.write(dataToWriteInZip(record.getMetadata().getMetadata(), fileFormat));
            writer.flush();
            zout.closeEntry();
        } catch (IOException e) {
            LOG.error("Error writing the zip entry", e);
        }
    }

    /**
     * converts the Record metadata into turtle if fileFormat is TTL.
     * Otherwise the return the default metadata
     *
     * @return metadata
     */
    private static String dataToWriteInZip(String metadata, String fileFormat) {
        if (StringUtils.equals(fileFormat, Constants.TTL_FILE)) {
            return TurtleResponseParser.generateTurtle(metadata);
        }
        return metadata;
    }

    /**
     * the name of the file depending upon the extension provided
     *
     * @return String
     */
    private static String getEntryName(Record record, String fileExtension) {
        String id = record.getHeader().getIdentifier();
        if (StringUtils.equals(fileExtension, Constants.TTL_FILE)) {
            return StringUtils.substringAfterLast(id, "/") + Constants.TTL_EXTENSION;
        }
        return StringUtils.substringAfterLast(id, "/") + Constants.XML_EXTENSION;
    }

    public static String getDirectoryName(String identifier) {
        String id = StringUtils.remove(identifier, Constants.BASE_URL);
        return id.split("/")[0];
    }

    /**
     * creates the md5Sum for the file
     */
    public static void createMD5SumFile(String filename) {
        createMD5SumFile(filename, filename);
    }

    /**
     * creates the md5Sum for the file when source and destination folder are different
     */
    public static void createMD5SumFile(String sourceFolder, String destinationFolder) {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(destinationFolder + Constants.MD5_EXTENSION))) {
            String checksum = getMD5Sum(sourceFolder);
            out.write(checksum + "\n");
        } catch (IOException e) {
            LOG.error("Error creating MD5Sum file", e);
        }
    }

    private static String getMD5Sum(String file) {
        MessageDigest digest;
        String checksum = null;
        try (InputStream is = new FileInputStream(new File(file))) {
            digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[1024];
            int read = 0;
            while ((read = is.read(buffer)) > 0) {
                digest.update(buffer, 0, read);
            }
            byte[] md5sum = digest.digest();
            BigInteger bigInt = new BigInteger(1, md5sum);
            checksum = bigInt.toString(16);
        } catch (IOException e) {
            LOG.error("Unable to process file for MD5", e);
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Error while generating MD5", e);
        }
        return checksum;
    }

    /**
     * returns the number of entries in a zip file
     * @param path
     * @param zipName
     * @return
     */
    public static long getNumberOfEntriesInZip(String path, String zipName) {
        try(ZipFile zipFile = new ZipFile(path + Constants.PATH_SEPERATOR + zipName)) {
            return zipFile.size();
        } catch (IOException e) {
            LOG.error("Error reading the zip file {}", zipName, e);
        }
        return 0;
    }
}

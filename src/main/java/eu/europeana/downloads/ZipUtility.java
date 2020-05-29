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
import java.util.zip.ZipOutputStream;

public class ZipUtility {

    private static final Logger LOG = LogManager.getLogger(ZipUtility.class);

    private ZipUtility() {
        //adding a private constructor to hide implicit public one
    }

    public static void writeInZip(ZipOutputStream zout, OutputStreamWriter writer, Record record) {
        try {
            zout.putNextEntry(new ZipEntry(getEntryName(record)));
            writer.write(record.getMetadata().getMetadata());
            writer.flush();
            zout.closeEntry();
        } catch (IOException e) {
            LOG.error("Error writing the zip entry", e);
        }

    }

    private static String getEntryName(Record record) {
        String id = record.getHeader().getIdentifier();
        return StringUtils.substringAfterLast(id, "/") + Constants.XML_EXTENSION;
    }

    public static String getDirectoryName(String identifier) {
        String id = StringUtils.remove(identifier, Constants.BASE_URL);
        return id.split("/")[0];
    }

    public static void createMD5SumFile(String fileName) {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(fileName + Constants.MD5_EXTENSION))) {
            String checksum = getMD5Sum(fileName);
            out.write(checksum + "\n");
        } catch (IOException e) {
            LOG.error("Error creating MD5Sum file", e);
        }
    }

    private static String getMD5Sum(String file) {
        MessageDigest digest;
        String checksum = null;
        try(InputStream is = new FileInputStream(new File(file))) {
            digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[1024];
            int read = 0;
            while( (read = is.read(buffer)) > 0) {
                digest.update(buffer, 0, read);
            }
            byte[] md5sum = digest.digest();
            BigInteger bigInt = new BigInteger(1, md5sum);
            checksum = bigInt.toString(16);
        }
        catch(IOException e) {
            LOG.error("Unable to process file for MD5", e);
        }
        catch (NoSuchAlgorithmException e) {
            LOG.error( "Error while generating MD5", e);
        }
        return checksum;
    }
}

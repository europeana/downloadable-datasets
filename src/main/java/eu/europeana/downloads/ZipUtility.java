package eu.europeana.downloads;

import eu.europeana.oaipmh.model.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.zip.CRC32;
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

    public static void createCRCFile(String fileName) {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(fileName + Constants.CRC_EXTENSION))) {
            long checksum = generateCRC(fileName);
            out.write(checksum + "\n");
        } catch (IOException e) {
            LOG.error("Error creating CRC file", e);
        }
    }

    private static long generateCRC(String fileName) {
        CRC32 checksum = new CRC32();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(fileName)))) {
            int read = 0;
            byte[] buffer = new byte[1024];
            while ((read = bis.read(buffer)) != -1) {
                checksum.update(buffer, 0, read);
            }
        } catch (IOException e) {
            LOG.error("Error generating CRC32", e);
        }
        return checksum.getValue();
    }
}

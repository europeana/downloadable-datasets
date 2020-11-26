package eu.europeana.downloads;

public class Constants {

    // verb
    public static final String GET_RECORD_VERB          = "GetRecord";
    public static final String LIST_IDENTIFIERS_VERB    = "ListIdentifiers";
    public static final String LIST_RECORDS_VERB        = "ListRecords";
    public static final String LIST_SET_VERB            = "ListSets";
    public static final String CHECKSUM_VERB            = "CheckSum";

    // Zip constants
    public static final String ZIP_EXTENSION            = ".zip";
    public static final String PATH_SEPERATOR           = "/";
    public static final String XML_EXTENSION            = ".xml";
    public static final String TTL_EXTENSION            = ".ttl";
    public static final String RDF_XML                  = "RDF/XML";

    public static final String BASE_URL                 = "http://data.europeana.eu/item/";

    // Constants for parsing XML Response
    public static final String HEADER_TAG               = "header";
    public static final String MEATADATA_TAG            = "metadata";
    public static final String RECORD_TAG               = "record";
    public static final String IDENTIFIER_TAG           = "identifier";
    public static final String SETSPEC_TAG              = "setSpec";
    public static final String DATESTAMP_TAG            = "datestamp";
    public static final String RESUMPTIONTOKEN_TAG      = "resumptionToken";
    public static final String COMPLETELISTSIZE_TAG     = "completeListSize";
    public static final String EXPIRATIONDATE_TAG       = "expirationDate";
    public static final String CURSOR_TAG               = "cursor";
    public static final String DATE_FORMAT              = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String XML_DECLARATION          = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";

    // Boolean Values
    public static final String TRUE                     = "true";
    public static final String FALSE                    = "false";

    // MD5 Constants
    public static final String MD5_EXTENSION            = ".md5sum";

    // File format
    public static final String XML_FILE                 = "XML";
    public static final String TTL_FILE                 = "TTL";

    public static final String HARVEST_DATE_FORMAT      = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String HARVEST_DATE_FILENAME    = "lastHarvestDate.txt";


    // CSV File Constants
    public static final String CSV_HEADER               = "Failed Sets";
    public static final String CSV_FILE                 = "FailedSetsReport";
    public static final String CSV_EXTENSION            = ".csv";
    public static final String REPORT_DATE_FORMAT       = "dd-MM-yyyy";

    // Mail Constants
    public static final String DOWNLOADS_SUBJECT = "Downloads Run Status Report for ";
    public static final String FAILED_SETS_RETRY_SUBJECT = "Failed Sets Retry Status Report for ";

}
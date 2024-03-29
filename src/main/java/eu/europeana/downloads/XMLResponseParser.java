package eu.europeana.downloads;

import eu.europeana.oaipmh.model.*;
import eu.europeana.oaipmh.model.Record;
import eu.europeana.oaipmh.model.response.GetRecordResponse;
import eu.europeana.oaipmh.model.response.ListRecordsResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class XMLResponseParser {

    private static final Logger LOG = LogManager.getLogger(XMLResponseParser.class);

    private XMLResponseParser() {
        //adding a private constructor to hide implicit public one
    }

    public static GetRecordResponse parseGetRecordResponse(String responseAsString) {
        GetRecordResponse recordResponse = new GetRecordResponse();
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        XMLEventReader eventReader = null;
        try (InputStream targetStream = new ByteArrayInputStream(responseAsString.getBytes())){
             eventReader = factory.createXMLEventReader(targetStream);

            GetRecord getRecord = new GetRecord();
            Record record = new Record();

            while (eventReader.hasNext()) {
                XMLEvent xmlEvent = eventReader.nextEvent();
                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    getTagsAndParse(eventReader, startElement,record, responseAsString);
                }
            }
            getRecord.setRecord(record);
            recordResponse.setGetRecord(getRecord);
        } catch (XMLStreamException e) {
            LOG.debug("Error parsing GetRecordResponse {} ", e);
        } catch (ParseException e) {
            LOG.debug("Error parsing Datestamp {} ", e);
        } catch (IOException e) {
            LOG.debug("Error creating the input stream {} ", e);
        }
        finally {
            if (eventReader != null) {
                try {
                    eventReader.close();
                } catch (XMLStreamException xse) {
                    // Ignore
                }
            }
        }
        return recordResponse;
    }

    public static ListRecordsResponse parseListRecordResponse(String responseAsString) {
        ListRecordsResponse recordResponse = new ListRecordsResponse();
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        XMLEventReader eventReader = null;
        try(InputStream targetStream = new ByteArrayInputStream(responseAsString.getBytes())) {
            eventReader = factory.createXMLEventReader(targetStream);

            ListRecords listRecords = new ListRecords();
            List<Record> recordList = new ArrayList<>();
            Record record = null;
            ResumptionToken resumptionToken = null;

            while (eventReader.hasNext()) {
                XMLEvent xmlEvent = eventReader.nextEvent();
                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    //As soo as record tag is opened, create new record object
                    if (Constants.RECORD_TAG.equalsIgnoreCase(startElement.getName().getLocalPart())) {
                        record = new Record();
                    }
                    getTagsAndParse(eventReader, startElement,record, responseAsString);
                    //get the resumption token value
                    if (Constants.RESUMPTIONTOKEN_TAG.equalsIgnoreCase(startElement.getName().getLocalPart())) {
                        resumptionToken = new ResumptionToken();
                        parseResumptionToken(startElement, resumptionToken);
                        resumptionToken.setValue(eventReader.getElementText());
                    }
                }
                if (xmlEvent.isEndElement() && StringUtils.equalsIgnoreCase(xmlEvent.asEndElement().getName().getLocalPart(), Constants.RECORD_TAG)) {
                    recordList.add(record);
                }
            }

            listRecords.setRecords(recordList);
            listRecords.setResumptionToken(resumptionToken);
            recordResponse.setListRecords(listRecords);
        } catch (XMLStreamException e) {
            LOG.debug("Error parsing ListRecordResponse {} ", e);
        } catch (ParseException e) {
            LOG.debug("Error parsing Datestamp {} ", e);
        } catch (IOException e) {
            LOG.debug("Error creating the input stream {} ", e);
        }
        finally {
            if (eventReader != null) {
                try {
                    eventReader.close();
                } catch (XMLStreamException xse) {
                    // Ignore
                }
            }
        }
        return recordResponse;
    }

    private static void getTagsAndParse(XMLEventReader eventReader, StartElement startElement, Record record, String response) throws XMLStreamException, ParseException {
        //get the header and metadata tag values
        switch (startElement.getName().getLocalPart()) {
            case Constants.HEADER_TAG:
                parseHeaderResource(eventReader, record);
                break;
            case Constants.MEATADATA_TAG:
                paraseMetadataString(response, record.getHeader().getIdentifier(), record);
                break;
            default: // do nothing
        }
    }
    private static void parseHeaderResource(XMLEventReader xmlEventReader, Record record) throws XMLStreamException, ParseException {
        Header header = new Header();
        record.setHeader(header);

        //get identifier, date, spetspec
        while (xmlEventReader.hasNext()) {
            XMLEvent e = xmlEventReader.nextEvent();
            if (e.isStartElement()) {
                StartElement se = e.asStartElement();

                switch (se.getName().getLocalPart()) {
                    case Constants.IDENTIFIER_TAG:
                        header.setIdentifier(xmlEventReader.getElementText());
                        break;
                    case Constants.DATESTAMP_TAG:
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constants.DATE_FORMAT);
                        Date parsedDate = simpleDateFormat.parse(xmlEventReader.getElementText());
                        header.setDatestamp(parsedDate);
                        break;
                    case Constants.SETSPEC_TAG:
                        header.setSetSpec(xmlEventReader.getElementText());
                        break;
                    default: // do nothing
                }
            }
            if (e.isEndElement() && StringUtils.equalsIgnoreCase(e.asEndElement().getName().getLocalPart(), Constants.HEADER_TAG)) {
                break;
            }
        }
    }

    private static void paraseMetadataString(String response, String identifier, Record record) {
        RDFMetadata metadata = new RDFMetadata();
        record.setMetadata(metadata);
        StringBuilder metadataValue = new StringBuilder(Constants.XML_DECLARATION);

        if (StringUtils.contains(response, identifier)) {
            // get the exact index of the identifier and  fetch the first metadata value from there. See : EA-3359
            int recordStartIndex = StringUtils.indexOf(response, "<identifier>" + identifier + "</identifier>");
            int metadataStartIndex = StringUtils.indexOf(response, "<metadata>", recordStartIndex);
            int metadataEndIndex = StringUtils.indexOf(response, "</metadata>", metadataStartIndex);
            String value = StringUtils.substring(response, metadataStartIndex + 10, metadataEndIndex);
            metadataValue.append(value);
        }
        metadata.setMetadata(metadataValue.toString());
    }

    private static void parseResumptionToken(StartElement startElement, ResumptionToken resumptionToken) throws ParseException {
        @SuppressWarnings("unchecked")
        Iterator<Attribute> iterator = startElement.getAttributes();

        while (iterator.hasNext()) {
            Attribute attribute = iterator.next();
            QName name = attribute.getName();
            if (Constants.COMPLETELISTSIZE_TAG.equalsIgnoreCase(name.getLocalPart())) {
                resumptionToken.setCompleteListSize(Integer.valueOf(attribute.getValue()));
            }
            if (Constants.EXPIRATIONDATE_TAG.equalsIgnoreCase(name.getLocalPart())) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constants.DATE_FORMAT);
                Date parsedDate = simpleDateFormat.parse(attribute.getValue());
                resumptionToken.setExpirationDate(parsedDate);
            }
            if (Constants.CURSOR_TAG.equalsIgnoreCase(name.getLocalPart())) {
                resumptionToken.setCursor(Integer.valueOf(attribute.getValue()));
            }

        }
    }
}

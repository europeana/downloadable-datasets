package eu.europeana.downloads;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

@Service
public class StatusReportService {
  private static final Logger LOG = LogManager.getLogger(StatusReportService.class);
   private final StatusReportConfig config;

  public StatusReportService(StatusReportConfig config) {
    this.config = config;
  }

  /**
   * Method publishes the report over the configured slack channel.
   * @param message -
   */
  public void publishStatusReportToSlack(String message) {
    LOG.info("Sending Slack Message : " + message);
    try {
       String slackWebhookApiAutomation = config.getSlackWebhook();
      if (StringUtils.isBlank(slackWebhookApiAutomation)) {
        LOG.error("Slack webhook not configured, status report will not be published over Slack.");
        return;
      }
      HttpPost httpPost = new HttpPost(slackWebhookApiAutomation);
      StringEntity entity   = new StringEntity(message);
      httpPost.setEntity(entity);
      httpPost.setHeader("Accept", "application/json");
      httpPost.setHeader("Content-type", "application/json");
      try (CloseableHttpClient httpClient = HttpClients.createDefault();
          CloseableHttpResponse response = httpClient.execute(httpPost)) {
        LOG.info("Received status " + response.getStatusLine().getStatusCode()
            + " while calling slack!");
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
          LOG.info(" Successfully sent slack message !");
        }
      }
    } catch (IOException e) {
      LOG.error("Exception occurred while sending slack message !! " + e.getMessage());
    }
  }

  public String uploadReportInCsv(String fileInput,String fileName) {
    try (FileWriter csvWriter = new FileWriter(fileName)) {
        csvWriter.append(fileInput);
      } catch (IOException e) {
        LOG.error("Exception occured during file Creation. {}",e.getMessage());
      }
      LOG.info("Report file {} ",fileName);
      return fileName;
    }

  private static String getReportPath(String directoryLocation,String fileName) {
     return directoryLocation + Constants.PATH_SEPERATOR +fileName;
  }

  private static String getReportFileName() {

    String fileNameSuffix = new SimpleDateFormat("yyyy_MM_dd").format(new Date());
    return Constants.DOWNLOAD_STATUS_FILENAME_PREFIX + fileNameSuffix+Constants.CSV_EXTENSION;
  }

  public void publishStatusReport(DownloadsStatus status, String subject, String directoryLocation,String downloadServerURL) {
    StringBuilder fileInput=new StringBuilder();
    StringBuilder jsonInput=new StringBuilder();
    String reportFileName = getReportFileName();//generate It ones as it contains timestamp
    String reportFileUrl = downloadServerURL + Constants.PATH_SEPERATOR + reportFileName;
    SetsUtility.generateReportsInput(status,subject,fileInput,jsonInput, reportFileUrl);
    uploadReportInCsv(fileInput.toString(),getReportPath(directoryLocation,reportFileName));
    publishStatusReportToSlack(jsonInput.toString());
  }
}

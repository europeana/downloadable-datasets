package eu.europeana.downloads;

import java.io.IOException;
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

}

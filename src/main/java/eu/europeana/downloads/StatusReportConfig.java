package eu.europeana.downloads;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StatusReportConfig {
  @Value("${slack-webhook}")
  private String slackWebhook;

  public String getSlackWebhook() {
    return slackWebhook;
  }
}

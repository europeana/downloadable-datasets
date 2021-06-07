package eu.europeana.downloads;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.SimpleMailMessage;

@Configuration
class MailConfig {

    @Value("${europeana-mail-sent-from}")
    private String sentFrom;

    @Value("${europeana-mail-copy-to}")
    private String copyTo;

    @Value("${europeana-mail-sent-to}")
    private String sentTo;

    @Bean
    public SimpleMailMessage emailTemplate() {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(sentFrom);
        message.setTo(sentTo);
        if (StringUtils.isNotEmpty(copyTo)) {
            message.setCc(copyTo);
        }
        message.setText(
                "Downloads Status Report :" +
                "%n%n" +
                "Number of datasets: %s" +
                "%n%n" +
                "Start Time:  %s" +
                "%n%n" +
                "Elapsed Time: %s" +
                "%n%n" +
                "Datasets Harvested: %s" +
                "%n%n" +
                "%s" +
                "%n%n" +
                "The Europeana API Team");
        return message;
    }
}

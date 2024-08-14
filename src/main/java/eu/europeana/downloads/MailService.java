package eu.europeana.downloads;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;

import org.springframework.stereotype.Service;

@Service
public class MailService {
    private static final Logger LOG = LogManager.getLogger(MailService.class);

  //  @Autowired
  //  private JavaMailSender mailSender;

    /**
     * This method will send compose and send the message
     */
    private void sendSimpleMessage(String from, String[] cc, String[] to, String subject, String messageBody) {
        LOG.debug("Sending email ...");
        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(from);
            message.setCc(cc);
            message.setTo(to);
            message.setSubject(subject);
            message.setText(messageBody);

           // mailSender.send(message);
        } catch (MailException e) {
            LOG.error("A problem prevented sending a confirmation {} email to {}", subject, to, e);
        }
    }

    public void sendSimpleMessageUsingTemplate(String subject,
                                               SimpleMailMessage template,
                                               String... templateArgs) {
        String messageBody = String.format(template.getText(), templateArgs);
        sendSimpleMessage(template.getFrom(), template.getCc(), template.getTo(), subject, messageBody);
    }
}
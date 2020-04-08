package eu.europeana.downloads;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("classpath:downloads.properties")
@PropertySource(value = "classpath:downloads.user.properties", ignoreResourceNotFound = true)
public class DownloadsGenerator implements CommandLineRunner {

    @Autowired
    private OAIPMHServiceClient oaipmhServiceClient;

    @Override
    public void run(String... args) throws Exception {
        if (StringUtils.isEmpty(oaipmhServiceClient.getHarvestMethod())) {
            throw new IllegalArgumentException("Please specify a harvest method (e.g. ListIdentifiers, ListRecords)");
        }
        oaipmhServiceClient.execute(oaipmhServiceClient.getHarvestMethod());
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder().sources(DownloadsGenerator.class).web(WebApplicationType.NONE).run(args);
    }
}

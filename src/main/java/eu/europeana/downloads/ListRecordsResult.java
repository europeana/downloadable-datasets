package eu.europeana.downloads;

import java.util.Map;

public class ListRecordsResult {
    private int errors;
    private String setsDownloaded;

    // execution time
    private float time;

    private  Map<String, String> failedRecordCountPerSet;

    ListRecordsResult(float time, String setsDownloaded, int errors,
        Map<String, String> failedRecordCountPerSet) {
        this.time = time;
        this.setsDownloaded = setsDownloaded;
        this.errors = errors;
        this.failedRecordCountPerSet = failedRecordCountPerSet;
    }

    int getErrors() {
        return errors;
    }

    public String getSetsDownloaded() {
        return setsDownloaded;
    }

    float getTime() {
        return time;
    }

    public Map<String, String> getFailedRecordCountPerSet() {
        return failedRecordCountPerSet;
    }
}

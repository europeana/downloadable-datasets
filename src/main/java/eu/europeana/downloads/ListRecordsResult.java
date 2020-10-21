package eu.europeana.downloads;

public class ListRecordsResult {
    private int errors;
    private String setsDownloaded;

    // execution time
    private float time;

    ListRecordsResult(float time, String setsDownloaded, int errors) {
        this.time = time;
        this.setsDownloaded = setsDownloaded;
        this.errors = errors;
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
}

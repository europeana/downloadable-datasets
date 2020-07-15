package eu.europeana.downloads;

public class ListRecordsResult {
    private int errors;
    private String setsDownloaded;

    // execution time
    private float time;

    ListRecordsResult(float time, String setsSuccess, int errors) {
        this.time = time;
        this.setsDownloaded = setsSuccess;
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

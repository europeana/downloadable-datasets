package eu.europeana.downloads;

import java.util.Date;

public class DownloadsStatus {

    private int noOfSets;
    private int setsHarvested;
    private Date startTime;
    private String timeElapsed;

    public DownloadsStatus() {

    }

    public DownloadsStatus(int noOfSets, int setsHarvested, Date startTime) {
        this.noOfSets = noOfSets;
        this.setsHarvested = setsHarvested;
        this.startTime = startTime;
    }

    public int getNoOfSets() {
        return noOfSets;
    }

    public void setNoOfSets(int noOfSets) {
        this.noOfSets = noOfSets;
    }

    public int getSetsHarvested() {
        return setsHarvested;
    }

    public void setSetsHarvested(int setsHarvested) {
        this.setsHarvested = setsHarvested;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public String getTimeElapsed() {
        return timeElapsed;
    }

    public void setTimeElapsed(String timeElapsed) {
        this.timeElapsed = timeElapsed;
    }

}


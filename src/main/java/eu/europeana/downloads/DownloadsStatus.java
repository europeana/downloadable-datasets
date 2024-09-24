package eu.europeana.downloads;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DownloadsStatus {

    private int noOfSets;
    private int setsHarvested;
    private Date startTime;
    private String timeElapsed;
    private String retriedSetsStatus;
    Map<String, Long> setsRecordCountMap = new HashMap<>();

    Map<String,ZipFileStatus> setsFileStatusMap = new HashMap<>();

    Map<String,String> failedRecordsCountMap = new HashMap<>();

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

    public String getRetriedSetsStatus() {
        return retriedSetsStatus;
    }

    public void setRetriedSetsStatus(String retriedSetsStatus) {
        this.retriedSetsStatus = retriedSetsStatus;
    }

    public Map<String, Long> getSetsRecordCountMap() {
        return setsRecordCountMap;
    }

    public void setSetsRecordCountMap(Map<String, Long> setsRecordCountMap) {
        this.setsRecordCountMap = setsRecordCountMap;
    }

    public void setsFileStatusMap(Map<String, ZipFileStatus> fileStatusMap) {
        this.setsFileStatusMap=fileStatusMap;
    }

    public Map<String, ZipFileStatus> getSetsFileStatusMap() {
        return setsFileStatusMap;
    }
    public void setFailedRecordsCountMap(Map<String, String> failedRecordsCountMap) {
        this.failedRecordsCountMap = failedRecordsCountMap;
    }
    public Map<String, String> getFailedRecordsCountMap() {
        return failedRecordsCountMap;
    }

    public void setSetsFailedRecordsCountMap(
        Map<String, String> setsFailedRecordsCountMap) {
        this.failedRecordsCountMap = setsFailedRecordsCountMap;
    }

    @Override
    public String toString() {
        return "DownloadsStatus{" +
            "noOfSets=" + noOfSets +
            ", setsHarvested=" + setsHarvested +
            ", startTime=" + startTime +
            ", timeElapsed='" + timeElapsed + '\'' +
            ", retriedSetsStatus='" + retriedSetsStatus + '\'' +
            ", setsRecordCountMap=" + setsRecordCountMap +
            ", setsFileStatusMap=" + setsFileStatusMap +
            '}';
    }
}


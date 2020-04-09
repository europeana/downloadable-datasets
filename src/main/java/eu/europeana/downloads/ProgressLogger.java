package eu.europeana.downloads;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 * Utility class to log progress of long harvests
 * @author Patrick Ehlert
 * Created on 30-03-2018
 */
public class ProgressLogger {

    private static final Logger LOG = LogManager.getLogger(ProgressLogger.class);

    private String setName;
    private long startTime;
    private long totalItems;
    private int logAfterSeconds;
    private long lastLogTime;

    /**
     * Create a new progressLogger. This also sets the operation start time
     * @param setName name of the set that is downloaded
     * @param totalItems total number of items that are expected to be retrieved
     * @param logAfterSeconds to prevent too much logging, only log every x seconds
     */
    public ProgressLogger(String setName, long totalItems, int logAfterSeconds) {
        this.setName = setName;
        this.startTime = System.currentTimeMillis();
        this.lastLogTime = startTime;
        this.totalItems = totalItems;
        this.logAfterSeconds = logAfterSeconds;
        LOG.debug("Create logger for {}", setName);
    }

    public void setTotalItems(long totalItems) {
        this.totalItems = totalItems;
    }

    /**
     * Log the number of items that are left to retrieve and an estimate of the remaining time, but only every x seconds
     * as specified by logAfterSeconds
     * @param itemsDone the number of items that have been retrieved
     */
    public void logProgress(long itemsDone) {
        LOG.debug("Check progress for {} ", setName);
        Duration d = new Duration(lastLogTime, System.currentTimeMillis());
        if (logAfterSeconds > 0 && d.getMillis() / 1000 > logAfterSeconds) {
            if (totalItems > 0) {
                double itemsPerMS = itemsDone * 1D / (System.currentTimeMillis() - startTime);
                if (itemsPerMS * 1000 > 1.5) {
                    LOG.info("Set {} - Retrieved {} items of {} ({} records/sec). Time remaining is {}", setName, itemsDone, totalItems,
                            Math.round(itemsPerMS * 1000), getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                } else  {
                    LOG.info("Set {} - Retrieved {} items of {} ({} records/min). Time remaining is {}", setName, itemsDone, totalItems,
                            Math.round(itemsPerMS * 1000 * 60), getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                }
            } else {
                LOG.info("Set {} - Retrieved {} items", setName, itemsDone);
            }
            lastLogTime = System.currentTimeMillis();
        }
    }

    /**
     * @param durationInMs
     * @return string containing duration in easy readable format
     */
    public static String getDurationText(long durationInMs) {
        String result;
        Period period = new Period(durationInMs);
        if (period.getDays() >= 1) {
            result = String.format("%d days, %d hours and %d minutes", period.getDays(), period.getHours(), period.getMinutes());
        } else if (period.getHours() >= 1) {
            result = String.format("%d hours and %d minutes", period.getHours(), period.getMinutes());
        } else if (period.getMinutes() >= 1){
            result = String.format("%d minutes and %d seconds", period.getMinutes(), period.getSeconds());
        } else {
            result = String.format("%d.%d seconds", period.getSeconds(), period.getMillis());
        }
        return result;
    }
}

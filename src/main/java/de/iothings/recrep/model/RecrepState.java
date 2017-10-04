package de.iothings.recrep.model;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Map;

/**
 * Created by ue60219 on 21.02.2017.
 */
public class RecrepState {


    private Map<String, JsonObject> scheduledRecordJobs;

    private Map<String, JsonObject> runningRecordJobs;

    private Map<String, JsonObject> recordJobs;

    private Map<String, JsonObject> scheduledReplayJobs;

    private Map<String, JsonObject> runningReplayJobs;

    private Map<String, JsonObject> replayJobs;


    public Map<String, JsonObject> getScheduledRecordJobs() {
        return scheduledRecordJobs;
    }

    public void setScheduledRecordJobs(Map<String, JsonObject> scheduledRecordJobs) {
        this.scheduledRecordJobs = scheduledRecordJobs;
    }

    public Map<String, JsonObject> getRunningRecordJobs() {
        return runningRecordJobs;
    }

    public void setRunningRecordJobs(Map<String, JsonObject> runningRecordJobs) {
        this.runningRecordJobs = runningRecordJobs;
    }

    public Map<String, JsonObject> getRecordJobs() {
        return recordJobs;
    }

    public void setRecordJobs(Map<String, JsonObject> recordJobs) {
        this.recordJobs = recordJobs;
    }

    public Map<String, JsonObject> getScheduledReplayJobs() {
        return scheduledReplayJobs;
    }

    public void setScheduledReplayJobs(Map<String, JsonObject> scheduledReplayJobs) {
        this.scheduledReplayJobs = scheduledReplayJobs;
    }

    public Map<String, JsonObject> getRunningReplayJobs() {
        return runningReplayJobs;
    }

    public void setRunningReplayJobs(Map<String, JsonObject> runningReplayJobs) {
        this.runningReplayJobs = runningReplayJobs;
    }

    public Map<String, JsonObject> getReplayJobs() {
        return replayJobs;
    }

    public void setReplayJobs(Map<String, JsonObject> replayJobs) {
        this.replayJobs = replayJobs;
    }
}

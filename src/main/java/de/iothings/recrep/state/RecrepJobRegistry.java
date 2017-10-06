package de.iothings.recrep.state;

import de.iothings.recrep.model.RecrepRecordJobFields;
import de.iothings.recrep.model.RecrepReplayJobFields;
import de.iothings.recrep.pubsub.MetricPublisher;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by johannes on 24.12.16.
 */
public class RecrepJobRegistry {

    public static HashMap<String, JsonObject> recordJobMap = new HashMap<>();
    public static HashMap<String, JsonObject> replayJobMap = new HashMap<>();

    public static  HashMap<String, MessageConsumer> recordStreamConsumerMap = new HashMap<String, MessageConsumer>();
    public static  HashMap<String, MessageConsumer> replayStreamConsumerMap = new HashMap<String, MessageConsumer>();

    public static HashMap<String, ArrayList<String>> recordStreamHandlerMap = new HashMap<>();
    public static HashMap<String, ArrayList<String>> replayStreamHandlerMap = new HashMap<>();

    public static HashMap<String, MetricPublisher> recordStreamMetricPublisherMap = new HashMap<>();

    public static void unregisterRecordStreamConsumer(String jobName) {
        if(recordStreamConsumerMap.containsKey(jobName)) {
            recordStreamConsumerMap.remove(jobName).unregister();
        }
    }

    public static void registerRecordStreamConsumer(String jobName, MessageConsumer messageConsumer) throws Exception {
        if(recordStreamConsumerMap.containsKey(jobName)) {
            throw new Exception("This job name is already registered.");
        }
        recordStreamConsumerMap.put(jobName, messageConsumer);
    }

    public static void unregisterReplayStreamConsumer(String jobName) {
        if(replayStreamConsumerMap.containsKey(jobName)) {
            replayStreamConsumerMap.remove(jobName).unregister();
        }
    }

    public static void registerReplayStreamConsumer(String jobName, MessageConsumer messageConsumer) throws Exception {
        if(replayStreamConsumerMap.containsKey(jobName)) {
            throw new Exception("This job name is already registered.");
        }
        replayStreamConsumerMap.put(jobName, messageConsumer);
    }


    public static void registerRecordStreamHandler(String jobName, String deploymentId) {
        if(recordStreamHandlerMap.containsKey(jobName)) {
            recordStreamHandlerMap.get(jobName).add(deploymentId);
        } else {
            recordStreamHandlerMap.put(jobName, new ArrayList<String>(Arrays.asList(deploymentId)));
        }
    }

    public static ArrayList<String> unregisterRecordStreamHandler(String jobName) {
        return recordStreamHandlerMap.remove(jobName);
    }

    public static void registerReplayStreamHandler(String jobName, String deploymentId) {
        if(replayStreamHandlerMap.containsKey(jobName)) {
            replayStreamHandlerMap.get(jobName).add(deploymentId);
        } else {
            replayStreamHandlerMap.put(jobName, new ArrayList<String>(Arrays.asList(deploymentId)));
        }
    }

    public static ArrayList<String> unregisterReplayStreamHandler(String jobName) {
        return replayStreamHandlerMap.remove(jobName);
    }

    public static void registerMetricPublisher(String jobName, MetricPublisher metricPublisher) {
        recordStreamMetricPublisherMap.put(jobName, metricPublisher);
    }

    public static void unregisterMetricPublisher(String jobName) {
        recordStreamMetricPublisherMap.remove(jobName);
    }

    public static void registerRecordJob(JsonObject recordJob) {
        recordJobMap.put(recordJob.getString(RecrepRecordJobFields.NAME), recordJob);
    }

    public static void unregisterRecordJob(String jobName) {
        recordJobMap.remove(jobName);
    }

    public static void registerReplayJob(JsonObject replayJob) {
        replayJobMap.put(replayJob.getString(RecrepReplayJobFields.NAME), replayJob);
    }

    public static void unregisterReplayJob(String jobName) {
        replayJobMap.remove(jobName);
    }

}

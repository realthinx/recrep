package de.iothings.recrep.state;

import io.vertx.core.eventbus.MessageConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by johannes on 24.12.16.
 */
public class RecrepJobRegistry {

    public static  HashMap<String, MessageConsumer> recordStreamConsumerMap = new HashMap<String, MessageConsumer>();
    public static  HashMap<String, MessageConsumer> replayStreamConsumerMap = new HashMap<String, MessageConsumer>();

    public static HashMap<String, ArrayList<String>> recordStreamHandlerMap = new HashMap<>();
    public static HashMap<String, ArrayList<String>> replayStreamHandlerMap = new HashMap<>();

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

}

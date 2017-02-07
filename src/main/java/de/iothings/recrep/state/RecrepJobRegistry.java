package de.iothings.recrep.state;

import io.vertx.core.eventbus.MessageConsumer;

import java.util.HashMap;

/**
 * Created by johannes on 24.12.16.
 */
public class RecrepJobRegistry {

    public static  HashMap<String, MessageConsumer> recordStreamConsumerMap = new HashMap<String, MessageConsumer>();
    public static  HashMap<String, MessageConsumer> replayStreamConsumerMap = new HashMap<String, MessageConsumer>();

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

}

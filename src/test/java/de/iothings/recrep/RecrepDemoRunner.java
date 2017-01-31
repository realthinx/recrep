package de.iothings.recrep;

import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

/**
 * Created by johannes on 27.12.16.
 */
public class RecrepDemoRunner {

    private static final Logger log = LoggerFactory.getLogger(RecrepDemoRunner.class.getName());

    private static Vertx vertx;
    private static EventPublisher eventPublisher;
    private static EventSubscriber eventSubscriber;
    private static Random random = new Random();
    private static Handler<JsonObject> requestReplayJobHandler = event -> sendDemoReplayJobRequest(event);

    public static void runDemo(Vertx vertx) {
        RecrepDemoRunner.vertx = vertx;
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());

//        eventSubscriber.subscribe(requestReplayJobHandler, RecrepEventType.RECORDSTREAM_ENDED);
//        subscribeToReplayStream();
        startTestDataStream("testdata_in");
        vertx.setTimer(2000, tick -> sendDemoRecordJobRequest());
    }

    private static void sendDemoRecordJobRequest() {

        long now = System.currentTimeMillis();
        long start = now + 2000;
        long end = now + 26000;
        JsonArray sources = new JsonArray();
        sources.add("testdata_in");

        JsonObject recordJob = new JsonObject();
        recordJob.put(RecrepRecordJobFields.NAME, "TEST_recording_job");
        recordJob.put(RecrepRecordJobFields.TIMESTAMP_START, start);
        recordJob.put(RecrepRecordJobFields.TIMESTAMP_END, end);
        recordJob.put(RecrepRecordJobFields.SOURCES, sources);
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_REQUEST, recordJob));
    }

    private static void startTestDataStream(String address) {
        vertx.setPeriodic(2000, tick -> {
            publishTestData(address);
        });
    }

    private static void publishTestData(String address) {
        vertx.setTimer(random.nextInt(500)+100, tick -> {
            JsonObject jsonObject = new JsonObject().put("index",tick).put("payload",new String(UUID.randomUUID().toString()));
            vertx.eventBus().publish(address, jsonObject);
            log.info("Record: " + jsonObject);
        });
    }

    private static void sendDemoReplayJobRequest(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);

        JsonObject targetMapping = new JsonObject();
        recordJob.getJsonArray(RecrepRecordJobFields.SOURCES).forEach(source -> {
            targetMapping.put(source.toString(),source.toString()+"_replay");
        });

        JsonObject replayJob = new JsonObject();
        replayJob.put(RecrepReplayJobFields.NAME, "TEST_replay_job");
        replayJob.put(RecrepReplayJobFields.RECORDJOBNAME, recordJob.getString(RecrepRecordJobFields.NAME));
        replayJob.put(RecrepReplayJobFields.TARGET_MAPPING, targetMapping);
        replayJob.put(RecrepReplayJobFields.SPEEDFACTOR, 2);
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYJOB_REQUEST, replayJob));
    }

    private static void subscribeToReplayStream() {
        vertx.eventBus().consumer("testdata_in_replay", message -> {
            log.info("Replay" + message.body());
        });
    }

}

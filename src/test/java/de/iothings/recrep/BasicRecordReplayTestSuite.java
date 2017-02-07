package de.iothings.recrep;

import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.*;
import org.junit.runner.RunWith;
import java.util.*;

/**
 * Created by ue60219 on 31.01.2017.
 */


@RunWith(VertxUnitRunner.class)
public class BasicRecordReplayTestSuite {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;
    private long periodicTestDataId;

    private String testDataStreamAdress;;
    private String testRecordJobName;
    private String testRecordJobFilePath;

    private List<MessageConsumer> messageConsumerList = new ArrayList<>();

    @Before
    public void before(TestContext context) {
        Async async = context.async();

        testDataStreamAdress = "testdata_in";
        testRecordJobName = "recording_job_" + UUID.randomUUID().toString();
        testRecordJobFilePath = "./.temp";

        // delete old files
        cleanup(context);

        eventPublisher = new EventPublisher(rule.vertx());
        eventSubscriber = new EventSubscriber(rule.vertx(), EventBusAddress.RECREP_EVENTS.toString());
        List<String> deployableList = Collections.emptyList();
        RecrepEmbedded recrepEmbedded = new RecrepEmbedded(rule.vertx(), deployableList);
        recrepEmbedded.deploy(finished -> {
            async.complete();
        });

        // start test data stream
        periodicTestDataId = rule.vertx().setPeriodic(1000, this::sendTestData);
    }

    @Test
    public void testRecordJob(TestContext context) {
        Async async = context.async();
        sendDemoRecordJobRequest();

        Handler<JsonObject> endRecordStreamHandler = endEvent -> {
            JsonObject recordJob = endEvent.getJsonObject(RecrepEventFields.PAYLOAD);
            Buffer fileBuffer = rule.vertx().fileSystem().readFileBlocking(testRecordJobFilePath + "/" + testRecordJobName + ".log");
            System.out.println(fileBuffer.toString());
            context.assertFalse(fileBuffer.toString().isEmpty());
            sendDemoReplayJobRequest(recordJob);
        };

        Handler<JsonObject> endReplayStreamHandler = endEvent -> {

            messageConsumerList.forEach(MessageConsumer::unregister);
            async.complete();
        };

        messageConsumerList.add(eventSubscriber.subscribe(endRecordStreamHandler, RecrepEventType.RECORDJOB_FINISHED));
        messageConsumerList.add(eventSubscriber.subscribe(endReplayStreamHandler, RecrepEventType.REPLAYJOB_FINISHED));
    }


    @After
    public void after(TestContext context) {
        Async async = context.async();

        rule.vertx().cancelTimer(periodicTestDataId);
        rule.vertx().close( voidAsyncResult -> {
            async.complete();
        });
    }

    private void sendTestData(long tick) {
        JsonObject jsonObject = new JsonObject().put("index",tick).put("payload",new String(UUID.randomUUID().toString()));
        rule.vertx().eventBus().publish(testDataStreamAdress, jsonObject);
    }

    private void sendDemoRecordJobRequest() {
        long now = System.currentTimeMillis();
        long start = now + 1000;
        long end = now + 10000;
        JsonArray sources = new JsonArray();
        sources.add(testDataStreamAdress);

        JsonObject recordJob = new JsonObject();
        recordJob.put(RecrepRecordJobFields.NAME, testRecordJobName);
        recordJob.put(RecrepRecordJobFields.FILE_PATH, testRecordJobFilePath);
        recordJob.put(RecrepRecordJobFields.TIMESTAMP_START, start);
        recordJob.put(RecrepRecordJobFields.TIMESTAMP_END, end);
        recordJob.put(RecrepRecordJobFields.SOURCES, sources);
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_REQUEST, recordJob));
    }

    private void sendDemoReplayJobRequest(JsonObject recordJob) {

        JsonObject targetMapping = new JsonObject();
        recordJob.getJsonArray(RecrepRecordJobFields.SOURCES).forEach(source -> {
            targetMapping.put(source.toString(),source.toString()+"_replay");
            messageConsumerList.add(rule.vertx().eventBus().consumer(source.toString()+"_replay", message -> {
                System.out.println("Replay Message: " + System.currentTimeMillis() + " - " + message.body());
            }));
        });

        JsonObject replayJob = new JsonObject();
        replayJob.put(RecrepReplayJobFields.NAME, testRecordJobName);
        replayJob.put(RecrepReplayJobFields.FILE_PATH, testRecordJobFilePath);
        replayJob.put(RecrepReplayJobFields.RECORDJOBNAME, recordJob.getString(RecrepRecordJobFields.NAME));
        replayJob.put(RecrepReplayJobFields.TARGET_MAPPING, targetMapping);
        replayJob.put(RecrepReplayJobFields.SPEEDFACTOR, 2);
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYJOB_REQUEST, replayJob));

    }

    private void cleanup(TestContext context) {
        Async async = context.async();
        rule.vertx().fileSystem().deleteRecursive(testRecordJobFilePath, true, done -> {
            async.complete();
        });
    }

}

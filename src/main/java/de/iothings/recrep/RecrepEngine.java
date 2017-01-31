package de.iothings.recrep;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.iothings.recrep.model.*;
import de.iothings.recrep.state.RecrepJobRegistry;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import de.iothings.recrep.common.RecordLogHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.HashMap;

public class RecrepEngine extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(RecrepEngine.class.getName());
    private final Kryo kryo = new Kryo();
    private final Handler<JsonObject> startRecordStreamHandler = this::startRecordStream;
    private final Handler<JsonObject> endRecordStreamHandler = this::endRecordStream;
    private final Handler<JsonObject> startReplayStreamHandler = this::startReplayStream;
    private final Handler<JsonObject> endReplayStreamHandler = this::endReplayStream;

    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    @Override
    public void start() throws Exception {

        log.info("started RecrepEngine");
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        subscribeToReqrepEvents();
    }

    @Override
    public void stop() throws Exception {

        log.info("stopped RecrepEngine");

    }

    private void subscribeToReqrepEvents() {

        eventSubscriber.subscribe(startRecordStreamHandler, RecrepEventType.RECORDJOB_REQUEST);
        eventSubscriber.subscribe(endRecordStreamHandler, RecrepEventType.RECORDJOB_FINISHED);
        eventSubscriber.subscribe(startReplayStreamHandler, RecrepEventType.REPLAYJOB_REQUEST);
        eventSubscriber.subscribe(endReplayStreamHandler, RecrepEventType.REPLAYJOB_FINISHED);

    }

    private void startRecordStream(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        Logger recordLog = RecordLogHelper.createAndGetRecordLogger(recordJob.getString(RecrepRecordJobFields.NAME));

        MessageConsumer<JsonObject> recordStream = vertx.eventBus().consumer(recordJob.getString(RecrepRecordJobFields.NAME), message -> {
            recordLog.info(message.headers().get("source") + "|" + encodeObject(message.body()));
        });

        try {
            RecrepJobRegistry.registerRecordStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME),recordStream);
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDSTREAM_CREATED, recordJob));
        } catch (Exception x) {
            log.error("Failed to register message consumer for record stream: " + x.getMessage());
            recordStream.unregister();
            RecordLogHelper.removeRecordLogger(recordJob.getString(RecrepRecordJobFields.NAME));
        }
    }

    private void endRecordStream(JsonObject event) {
        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        RecrepJobRegistry.unregisterRecordStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME));
        RecordLogHelper.removeRecordLogger(recordJob.getString(RecrepRecordJobFields.NAME));
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDSTREAM_ENDED, recordJob));
    }

    private void startReplayStream(JsonObject event) {

        JsonObject replayJob = event.getJsonObject(RecrepEventFields.PAYLOAD);

        HashMap<String, MessageProducer<JsonObject>> messageProducers = new HashMap<>();
        replayJob.getJsonObject(RecrepReplayJobFields.TARGET_MAPPING).forEach(mapping -> {
            messageProducers.put(mapping.getKey(), vertx.eventBus().sender(mapping.getValue().toString()));

        });

        MessageConsumer<JsonObject> replayStream = vertx.eventBus().consumer(replayJob.getString(RecrepReplayJobFields.NAME), message -> {
            JsonObject recordLine = message.body();
            String source = recordLine.getString("source");
            messageProducers.get(source).send(decodeObject(recordLine.getString("payload")));
        });

        try {
            RecrepJobRegistry.registerRecordStreamConsumer(replayJob.getString(RecrepReplayJobFields.NAME),replayStream);
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYSTREAM_CREATED, replayJob));
        } catch (Exception x) {
            log.error("Failed to register message consumer for replay stream: " + x.getMessage());
            replayStream.unregister();
        }

    }

    private void endReplayStream(JsonObject event) {
        JsonObject replayJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        RecrepJobRegistry.unregisterRecordStreamConsumer(replayJob.getString(RecrepReplayJobFields.NAME));
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYSTREAM_ENDED, replayJob));
    }

    private String encodeObject(JsonObject object) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream);
        kryo.writeObject(output, object);
        output.close();
        return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
    }

    private JsonObject decodeObject(String object) {
        Input input = new Input(new ByteArrayInputStream(Base64.getDecoder().decode(object)));
        JsonObject jsonObject = kryo.readObject(input, JsonObject.class);
        input.close();
        return jsonObject;
    }
}

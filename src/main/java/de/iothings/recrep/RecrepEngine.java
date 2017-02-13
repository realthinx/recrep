package de.iothings.recrep;

import de.iothings.recrep.common.JobConfigHelper;
import de.iothings.recrep.common.RecordLogHelper;
import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import de.iothings.recrep.state.RecrepJobRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RecrepEngine extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(RecrepEngine.class.getName());
    private final Handler<JsonObject> startRecordStreamHandler = this::startRecordStream;
    private final Handler<JsonObject> saveRecordJobConfigHandler = this::saveJobConfig;
    private final Handler<JsonObject> endRecordStreamHandler = this::endRecordStream;
    private final Handler<JsonObject> startReplayStreamHandler = this::startReplayStream;
    private final Handler<JsonObject> saveReplayJobConfigHandler = this::saveJobConfig;
    private final Handler<JsonObject> endReplayStreamHandler = this::endReplayStream;

    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    private List<MessageConsumer> messageConsumerList = new ArrayList<>();

    @Override
    public void start() throws Exception {
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        subscribeToReqrepEvents();
        log.info("Started " + this.getClass().getName());
    }

    @Override
    public void stop() throws Exception {
        messageConsumerList.forEach(MessageConsumer::unregister);
        log.info("Stopped " + this.getClass().getName());
    }

    private void subscribeToReqrepEvents() {
        messageConsumerList.add(eventSubscriber.subscribe(startRecordStreamHandler, RecrepEventType.RECORDJOB_REQUEST));
        messageConsumerList.add(eventSubscriber.subscribe(saveRecordJobConfigHandler, RecrepEventType.RECORDJOB_REQUEST));
        messageConsumerList.add(eventSubscriber.subscribe(endRecordStreamHandler, RecrepEventType.RECORDJOB_FINISHED));
        messageConsumerList.add(eventSubscriber.subscribe(startReplayStreamHandler, RecrepEventType.REPLAYJOB_REQUEST));
        messageConsumerList.add(eventSubscriber.subscribe(saveReplayJobConfigHandler, RecrepEventType.REPLAYJOB_REQUEST));
        messageConsumerList.add(eventSubscriber.subscribe(endReplayStreamHandler, RecrepEventType.REPLAYJOB_FINISHED));
    }

    private void startRecordStream(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        Logger recordLog = RecordLogHelper.createAndGetRecordLogger(recordJob);

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
        replayJob.getJsonArray(RecrepReplayJobFields.TARGET_MAPPINGS).forEach(mapping -> {
            JsonObject targetMapping = (JsonObject) mapping;
            String sourceIdentifier = targetMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER);
            String targetIdentifier = targetMapping.getString(RecrepEndpointMappingFields.TARGET_IDENTIFIER);
            messageProducers.put(sourceIdentifier, vertx.eventBus().sender(targetIdentifier));

        });

        MessageConsumer<JsonObject> replayStream = vertx.eventBus().consumer(replayJob.getString(RecrepReplayJobFields.NAME), message -> {
            JsonObject recordLine = message.body();
            String source = recordLine.getString("source");
            MessageProducer producer = messageProducers.get(source);
            if(producer != null) {
                producer.send(decodeObject(recordLine.getString("payload")));
            }
        });

        try {
            RecrepJobRegistry.registerReplayStreamConsumer(replayJob.getString(RecrepReplayJobFields.NAME),replayStream);
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYSTREAM_CREATED, replayJob));
        } catch (Exception x) {
            log.error("Failed to register message consumer for replay stream: " + x.getMessage());
            replayStream.unregister();
        }

    }

    private void saveJobConfig(JsonObject event) {
        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        JobConfigHelper.saveJobConfig(recordJob);
    }


    private void endReplayStream(JsonObject event) {
        JsonObject replayJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        RecrepJobRegistry.unregisterReplayStreamConsumer(replayJob.getString(RecrepReplayJobFields.NAME));
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYSTREAM_ENDED, replayJob));
    }

    private String encodeObject(JsonObject object) {
        return object.toString();
    }

    private JsonObject decodeObject(String object) {
        JsonObject jsonObject = new JsonObject(object);
        return jsonObject;
    }
}

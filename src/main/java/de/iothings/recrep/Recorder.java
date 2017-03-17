package de.iothings.recrep;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import de.iothings.recrep.state.RecrepJobRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;


public class Recorder extends AbstractVerticle {

    private RecrepLogHelper log;
    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;
    private List<MessageConsumer> messageConsumerList = new ArrayList<>();

    private Handler<JsonObject> startRecordJobHandler = event -> startRecordJob(event);

    @Override
    public void start() throws Exception {
        log = new RecrepLogHelper(vertx, Recorder.class.getName());
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        messageConsumerList.add(eventSubscriber.subscribe(startRecordJobHandler, RecrepEventType.RECORDSTREAM_CREATED));
        log.info("Started " + this.getClass().getName());
    }


    @Override
    public void stop() throws Exception {
        messageConsumerList.forEach(MessageConsumer::unregister);
        log.info("Stopped " + this.getClass().getName());
    }

    private void startRecordJob(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        final ArrayList<MessageConsumer<JsonObject>> dataStreamConsumer = new ArrayList<>();
        //final ArrayList<String> recordStreamHandler = new ArrayList<>();
        //final MessageConsumer<JsonObject>[] dataStreamConsumer = new MessageConsumer[recordJob.getJsonArray(RecrepRecordJobFields.SOURCES).size()];

        long now = System.currentTimeMillis();
        long start = recordJob.getLong(RecrepRecordJobFields.TIMESTAMP_START) - now;
        long end = recordJob.getLong(RecrepRecordJobFields.TIMESTAMP_END) - now;

        if(start >= 1 && end > start) {

            vertx.setTimer(start, tick -> {
                eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_STARTED, recordJob));
                recordJob.getJsonArray(RecrepRecordJobFields.SOURCE_MAPPINGS).forEach((mapping -> {
                    JsonObject sourceMapping = (JsonObject) mapping;

                    DeploymentOptions deploymentOptions = new DeploymentOptions()
                            .setConfig(
                                    new JsonObject()
                                        .put(RecrepRecordJobFields.NAME, recordJob.getString(RecrepRecordJobFields.NAME))
                                        .put(RecrepEndpointMappingFields.STAGE, sourceMapping.getString(RecrepEndpointMappingFields.STAGE))
                                        .put(RecrepEndpointMappingFields.HANDLER, sourceMapping.getString(RecrepEndpointMappingFields.HANDLER))
                                        .put(RecrepEndpointMappingFields.SOURCE_IDENTIFIER, sourceMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER)));

                    log.debug("Create consumer for address " + sourceMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER));

                    vertx.deployVerticle(sourceMapping.getString(RecrepEndpointMappingFields.HANDLER), deploymentOptions, deployementResult -> {
                        RecrepJobRegistry.registerRecordStreamHandler(recordJob.getString(RecrepRecordJobFields.NAME), deployementResult.result());
                    });

                }));
            });

            try {
                vertx.setTimer(end, tick -> {
                    RecrepJobRegistry.unregisterRecordStreamHandler(recordJob.getString(RecrepRecordJobFields.NAME)).forEach(deploymentID -> {
                        vertx.undeploy(deploymentID);
                    });
                    eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_FINISHED, recordJob));
                });
            } catch (IllegalArgumentException x) {
                log.warn(x.getMessage());
            }
        } else {
            log.warn("Discarding Record Job. Start time is in the past.");
        }
    }

//    private void startRecordJob(JsonObject event) {
//
//        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
//        final ArrayList<MessageConsumer<JsonObject>> dataStreamConsumer = new ArrayList<>();
//        //final MessageConsumer<JsonObject>[] dataStreamConsumer = new MessageConsumer[recordJob.getJsonArray(RecrepRecordJobFields.SOURCES).size()];
//
//        long now = System.currentTimeMillis();
//        long start = recordJob.getLong(RecrepRecordJobFields.TIMESTAMP_START) - now;
//        long end = recordJob.getLong(RecrepRecordJobFields.TIMESTAMP_END) - now;
//
//        if(start >= 1 && end > start) {
//
//            vertx.setTimer(start, tick -> {
//                eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_STARTED, recordJob));
//                recordJob.getJsonArray(RecrepRecordJobFields.SOURCE_MAPPINGS).forEach((mapping -> {
//                    JsonObject sourceMapping = (JsonObject) mapping;
//                    String sourceIdentifier = sourceMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER);
//                    log.debug("Create consumer for address " + sourceIdentifier);
//                    dataStreamConsumer.add(vertx.eventBus().consumer(sourceIdentifier, message -> {
//                        DeliveryOptions deliveryOptions = new DeliveryOptions();
//                        deliveryOptions.addHeader("source", sourceIdentifier);
//                        vertx.eventBus().send(recordJob.getString(RecrepRecordJobFields.NAME), message.body(), deliveryOptions);
//                    }));
//                }));
//            });
//
//            try {
//                vertx.setTimer(end, tick -> {
//                    dataStreamConsumer.forEach(MessageConsumer::unregister);
//                    eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_FINISHED, recordJob));
//                });
//            } catch (IllegalArgumentException x) {
//                log.warn(x.getMessage());
//            }
//        } else {
//            log.warn("Discarding Record Job. Start time is in the past.");
//        }
//    }
}

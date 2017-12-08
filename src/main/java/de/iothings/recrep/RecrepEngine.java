package de.iothings.recrep;

import de.iothings.recrep.common.EndpointConfigHelper;
import de.iothings.recrep.common.JobConfigHelper;
import de.iothings.recrep.common.RecordLogHelper;
import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import de.iothings.recrep.pubsub.MetricPublisher;
import de.iothings.recrep.state.RecrepJobRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableHandler;
import io.vertx.rx.java.RxHelper;
import org.slf4j.Logger;
import rx.Observable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RecrepEngine extends AbstractVerticle {

    private RecrepLogHelper log;
    private RecordLogHelper recordLogHelper;
    private JsonObject recrepConfiguration;

    private final Handler<JsonObject> startRecordStreamHandler = this::startRecordStream;
    //private final Handler<JsonObject> startRecordJobAnalysisStreamHandler = this::startRecordJobAnalysisStream;
    private final Handler<JsonObject> saveRecordJobConfigHandler = this::saveJobConfig;
    private final Handler<JsonObject> endRecordStreamHandler = this::endRecordStream;
    private final Handler<JsonObject> startReplayStreamHandler = this::startReplayStream;
    private final Handler<JsonObject> endReplayStreamHandler = this::endReplayStream;
    private Handler<JsonObject> configurationUpdateHandler = this::handleConfigurationUpdate;


    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    @Override
    public void start() throws Exception {
        log = new RecrepLogHelper(vertx, RecrepEngine.class.getName());
        recordLogHelper = new RecordLogHelper(vertx);
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        subscribeToReqrepEvents();
        initializeConfiguration();
        monitorResources();
        log.info("Started " + this.getClass().getName());
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopped " + this.getClass().getName());
    }

    private void subscribeToReqrepEvents() {
        eventSubscriber.subscribe(startRecordStreamHandler, RecrepEventType.RECORDJOB_REQUEST);
        eventSubscriber.subscribe(saveRecordJobConfigHandler, RecrepEventType.RECORDJOB_REQUEST);
        eventSubscriber.subscribe(endRecordStreamHandler, RecrepEventType.RECORDJOB_FINISHED);
        eventSubscriber.subscribe(saveRecordJobConfigHandler, RecrepEventType.RECORDJOB_FINISHED);
        //eventSubscriber.subscribe(endRecordStreamHandler, RecrepEventType.RECORDJOB_CANCEL_REQUEST);
        //eventSubscriber.subscribe(startRecordJobAnalysisStreamHandler, RecrepEventType.RECORDJOB_ANALYSIS_REQUEST);
        eventSubscriber.subscribe(startReplayStreamHandler, RecrepEventType.REPLAYJOB_REQUEST);
        eventSubscriber.subscribe(endReplayStreamHandler, RecrepEventType.REPLAYJOB_FINISHED);
        eventSubscriber.subscribe(endReplayStreamHandler, RecrepEventType.REPLAYJOB_CANCEL_REQUEST);
        eventSubscriber.subscribe(configurationUpdateHandler, RecrepEventType.CONFIGURATION_UPDATE);
    }

    private void initializeConfiguration() {
        vertx.eventBus().send(EventBusAddress.CONFIGURATION_REQUEST.toString(), new JsonObject(), configurationReply -> {
            if(recrepConfiguration == null) {
                restartUnfinishedJobs();
            }
            recrepConfiguration = (JsonObject) configurationReply.result().body();
        });
    }

    private void startRecordStream(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        Logger recordLog = recordLogHelper.createAndGetRecordLogger(recordJob);
        MetricPublisher metricPublisher = new MetricPublisher(vertx, recordJob);

        MessageConsumer<JsonObject> recordStream = vertx.eventBus().consumer(recordJob.getString(RecrepRecordJobFields.NAME), message -> {
            String payload = encodeObject(message.body());
            recordLog.info(message.headers().get("source") + "|" + payload);
            // metricPublisher.publishMessageCount(message.headers().get("source"));
            metricPublisher.publishMessageMetrics(message.headers().get("source"), payload.getBytes().length);
        });

        try {
            RecrepJobRegistry.registerRecordJob(recordJob);
            RecrepJobRegistry.registerRecordStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME),recordStream);
            RecrepJobRegistry.registerMetricPublisher(recordJob.getString(RecrepRecordJobFields.NAME),metricPublisher);
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDSTREAM_CREATED, recordJob));
        } catch (Exception x) {
            log.error("Failed to register message consumer for record stream: " + x.getMessage());
            recordStream.unregister();
            recordLogHelper.removeRecordLogger(recordJob.getString(RecrepRecordJobFields.NAME));
        }
    }


    private void endRecordStream(JsonObject event) {
        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        RecrepJobRegistry.unregisterRecordStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME));
        RecrepJobRegistry.unregisterMetricPublisher(recordJob.getString(RecrepRecordJobFields.NAME));
        RecrepJobRegistry.unregisterRecordJob(recordJob.getString(RecrepRecordJobFields.NAME));
        recordLogHelper.removeRecordLogger(recordJob.getString(RecrepRecordJobFields.NAME));
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDSTREAM_ENDED, recordJob));
    }

    private void startReplayStream(JsonObject event) {

        JsonObject replayJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        MetricPublisher metricPublisher = new MetricPublisher(vertx, replayJob);

        ArrayList<ObservableHandler<Message<String>>> endpointReadyTriggers = new ArrayList<>();
        HashMap<String, MessageProducer<JsonObject>> messageProducers = new HashMap<>();
        replayJob.getJsonArray(RecrepReplayJobFields.TARGET_MAPPINGS).forEach(mapping -> {
            JsonObject targetMapping = (JsonObject) mapping;

            JsonObject endpointConfig = EndpointConfigHelper.lookupReplayHandlerProperties(recrepConfiguration,
                    targetMapping.getString(RecrepEndpointMappingFields.HANDLER),
                    targetMapping.getString(RecrepEndpointMappingFields.STAGE),
                    targetMapping.getString(RecrepEndpointMappingFields.TARGET_IDENTIFIER));

            if(endpointConfig != null) {

                DeploymentOptions deploymentOptions = new DeploymentOptions()
                        .setConfig(
                                new JsonObject()
                                        .put(RecrepReplayJobFields.NAME, replayJob.getString(RecrepReplayJobFields.NAME))
                                        .put(RecrepEndpointMappingFields.STAGE, targetMapping.getString(RecrepEndpointMappingFields.STAGE))
                                        .put(RecrepEndpointMappingFields.HANDLER, targetMapping.getString(RecrepEndpointMappingFields.HANDLER))
                                        .put(RecrepEndpointMappingFields.SOURCE_IDENTIFIER, targetMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER))
                                        .put(RecrepEndpointMappingFields.TARGET_IDENTIFIER, targetMapping.getString(RecrepEndpointMappingFields.TARGET_IDENTIFIER))
                                        .put(RecrepEndpointMappingFields.PROPERTIES, endpointConfig.getJsonObject(RecrepEndpointMappingFields.PROPERTIES)));

                ObservableHandler<Message<String>> endpointReadyHandler = RxHelper.observableHandler();
                endpointReadyTriggers.add(endpointReadyHandler);
                String eventBusAddress = replayJob.getString(RecrepReplayJobFields.NAME) + "_"
                        + targetMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER);
                vertx.eventBus().consumer(eventBusAddress + "_" + RecrepSignalType.REPLAYENDPOINT_READY, endpointReadyHandler.toHandler());

                vertx.deployVerticle(targetMapping.getString(RecrepEndpointMappingFields.HANDLER), deploymentOptions, deployementResult -> {
                    if(deployementResult.failed()) {
                        log.warn("Discarding Replay Job. Failed to start endpoint handler.");
                        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYJOB_FINISHED, replayJob));
                    } else {
                        RecrepJobRegistry.registerReplayStreamHandler(replayJob.getString(RecrepReplayJobFields.NAME), deployementResult.result());
                        messageProducers.put(targetMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER),
                                vertx.eventBus().sender(eventBusAddress));
                    }
                });
            } else {
                log.warn("Discarding Replay Job. Failed to lookup endpoint configuration.");
                eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYJOB_FINISHED, replayJob));
            }

        });

        MessageConsumer<JsonObject> replayStream = vertx.eventBus().consumer(replayJob.getString(RecrepReplayJobFields.NAME), message -> {
            JsonObject recordLine = message.body();
            String source = recordLine.getString(RecrepRecordMessageFields.SOURCE);
            MessageProducer producer = messageProducers.get(source);
            if(producer != null) {
                JsonObject payload = decodeObject(recordLine.getString(RecrepRecordMessageFields.PAYLOAD));
                producer.send(payload);
                metricPublisher.publishMessageMetrics(source, payload.toString().getBytes().length);
            }
        });

        try {
            RecrepJobRegistry.registerReplayStreamConsumer(replayJob.getString(RecrepReplayJobFields.NAME),replayStream);
            // Observe endpoint ready signals and emit when all endpoints are ready
            Observable.merge(endpointReadyTriggers.toArray(new ObservableHandler[endpointReadyTriggers.size()]))
                    .buffer(endpointReadyTriggers.size())
                    .timeout(10, TimeUnit.SECONDS)
                    .subscribe( allReady -> {
                        log.debug("All endpoints ready for Job: " + replayJob);
                        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYSTREAM_CREATED, replayJob));
                    }, error -> {
                        log.warn("Timeout waiting for endpoints to be ready for Job: " + replayJob);
                        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYJOB_FINISHED, replayJob));
                    });
        } catch (Exception x) {
            log.error("Failed to register replay stream: " + x.getMessage());
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

        ArrayList<String> deploymentIds = RecrepJobRegistry.unregisterReplayStreamHandler(replayJob.getString(RecrepReplayJobFields.NAME));
        if(deploymentIds != null) {
            deploymentIds.forEach(deploymentID -> {
                vertx.undeploy(deploymentID);
            });
        }
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYSTREAM_ENDED, replayJob));
    }

    private void handleConfigurationUpdate(JsonObject event) {
        recrepConfiguration = event.getJsonObject(RecrepEventFields.PAYLOAD);
    }

    private String encodeObject(JsonObject object) {
        return object.toString();
    }

    private JsonObject decodeObject(String object) {
        JsonObject jsonObject = new JsonObject(object);
        return jsonObject;
    }

    private void restartUnfinishedJobs() {
        vertx.eventBus().send(EventBusAddress.STATE_REQUEST.toString(), new JsonObject(), state -> {

            JsonObject currentState = (JsonObject) state.result().body();
            currentState.getJsonObject(RecrepEventFields.PAYLOAD).getJsonArray("recordJobs").forEach(job -> {
               JsonObject recordJob = (JsonObject) job;
                Long timestampEnd = recordJob.getLong(RecrepRecordJobFields.TIMESTAMP_END);
               if(timestampEnd == null || timestampEnd > System.currentTimeMillis()) {
                   log.info("Restarting unfinished job: " + recordJob.getString(RecrepRecordJobFields.NAME));
                   startRecordStream(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_REQUEST, recordJob));
               }
            });
        });

    }

    private void monitorResources() {
        vertx.setPeriodic(10000l, tick -> {
            vertx.eventBus().send(EventBusAddress.STATE_REQUEST.toString(), new JsonObject(), state -> {
                JsonObject currentState = (JsonObject) state.result().body();
                currentState.getJsonObject(RecrepEventFields.PAYLOAD).getJsonArray("activeRecordJobs").forEach( recordJob -> {
                    JsonObject job = (JsonObject) recordJob;
                    MetricPublisher metricPublisher = RecrepJobRegistry.recordStreamMetricPublisherMap.get(job.getString(RecrepRecordJobFields.NAME));
                    if(metricPublisher != null) {
                        metricPublisher.countResourceMetrics();
                    }
                });
            });
        });
    }


}

package de.iothings.recrep;

import de.iothings.recrep.common.EndpointConfigHelper;
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


public class Recorder extends AbstractVerticle {

    private RecrepLogHelper log;
    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;
    private JsonObject recrepConfiguration;

    private long timerId;

    private Handler<JsonObject> startRecordJobHandler = event -> startRecordJob(event);
    private Handler<JsonObject> configurationUpdateHandler = this::handleConfigurationUpdate;
    private Handler<JsonObject> cancelRecordJobHandler = this::cancelRecordJobHandler;

    @Override
    public void start() throws Exception {
        log = new RecrepLogHelper(vertx, Recorder.class.getName());
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        subscribeToReqrepEvents();
        initializeConfiguration();
        log.info("Started " + this.getClass().getName());
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopped " + this.getClass().getName());
    }

    private void initializeConfiguration() {
        vertx.eventBus().send(EventBusAddress.CONFIGURATION_REQUEST.toString(), new JsonObject(), configurationReply -> {
            recrepConfiguration = (JsonObject) configurationReply.result().body();
        });
    }

    private void subscribeToReqrepEvents() {
        eventSubscriber.subscribe(startRecordJobHandler, RecrepEventType.RECORDSTREAM_CREATED);
        eventSubscriber.subscribe(configurationUpdateHandler, RecrepEventType.CONFIGURATION_UPDATE);
        eventSubscriber.subscribe(cancelRecordJobHandler, RecrepEventType.RECORDJOB_CANCEL_REQUEST);
    }

    private void handleConfigurationUpdate(JsonObject event) {
        recrepConfiguration = event.getJsonObject(RecrepEventFields.PAYLOAD);
    }

    private void startRecordJob(JsonObject event) {

        JsonObject recordJob = RecrepJobRegistry.recordJobMap.get(event.getJsonObject(RecrepEventFields.PAYLOAD).getString(RecrepRecordJobFields.NAME));
        final ArrayList<MessageConsumer<JsonObject>> dataStreamConsumer = new ArrayList<>();

        long now = System.currentTimeMillis();
        long start = recordJob.getLong(RecrepRecordJobFields.TIMESTAMP_START) - now;
        Long optionalEnd = recordJob.getLong(RecrepRecordJobFields.TIMESTAMP_END);
        long end = optionalEnd != null ? optionalEnd - now : start;

        vertx.eventBus().consumer(RecrepSignalType.METRICS + "-" + recordJob.getString(RecrepRecordJobFields.NAME),
                metricMessage -> appendMetric(recordJob, (JsonObject) metricMessage.body()));

        if(start >= 1 && end >= start) {

            vertx.setTimer(start, tick -> {
                eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_STARTED, recordJob));
                recordJob.getJsonArray(RecrepRecordJobFields.SOURCE_MAPPINGS).forEach((mapping -> {
                    JsonObject sourceMapping = (JsonObject) mapping;

                    JsonObject endpointConfig = EndpointConfigHelper.lookupRecordHandlerProperties(recrepConfiguration,
                            sourceMapping.getString(RecrepEndpointMappingFields.HANDLER),
                            sourceMapping.getString(RecrepEndpointMappingFields.STAGE),
                            sourceMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER));

                    if(endpointConfig != null) {
                        DeploymentOptions deploymentOptions = new DeploymentOptions()
                                .setConfig(
                                        new JsonObject()
                                                .put(RecrepRecordJobFields.NAME, recordJob.getString(RecrepRecordJobFields.NAME))
                                                .put(RecrepEndpointMappingFields.STAGE, sourceMapping.getString(RecrepEndpointMappingFields.STAGE))
                                                .put(RecrepEndpointMappingFields.HANDLER, sourceMapping.getString(RecrepEndpointMappingFields.HANDLER))
                                                .put(RecrepEndpointMappingFields.SOURCE_IDENTIFIER, sourceMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER))
                                                .put(RecrepEndpointMappingFields.PROPERTIES, endpointConfig.getJsonObject(RecrepEndpointMappingFields.PROPERTIES)));
                        // todo: not nice
                        if(sourceMapping.getJsonObject(RecrepEndpointMappingFields.PROPERTIES) != null) {
                            sourceMapping.getJsonObject(RecrepEndpointMappingFields.PROPERTIES).forEach(propertyEntry -> {
                                deploymentOptions.getConfig().getJsonObject(RecrepEndpointMappingFields.PROPERTIES)
                                        .put(propertyEntry.getKey(), propertyEntry.getValue());
                            });
                        }

                        log.debug("Create consumer for address " + sourceMapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER));

                        vertx.deployVerticle(sourceMapping.getString(RecrepEndpointMappingFields.HANDLER), deploymentOptions, deployementResult -> {
                            if(deployementResult.failed()) {
                                log.warn("Discarding Record Job. Failed to start endpoint handler.");
                                eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_FINISHED, recordJob));
                            } else {
                                RecrepJobRegistry.registerRecordStreamHandler(recordJob.getString(RecrepRecordJobFields.NAME), deployementResult.result());
                            }
                        });
                    } else {
                        log.warn("Discarding Record Job. Failed to lookup endpoint configuration.");
                        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_FINISHED, recordJob));
                    }

                }));
            });

            try {
                if (end > start) {
                    vertx.setTimer(end, tick -> {
                        if(RecrepJobRegistry.recordJobMap.containsKey(recordJob.getString(RecrepRecordJobFields.NAME))) {
                            RecrepJobRegistry.unregisterRecordStreamHandler(recordJob.getString(RecrepRecordJobFields.NAME)).forEach(deploymentID -> {
                                vertx.undeploy(deploymentID);
                            });
                            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_FINISHED, recordJob));
                        }
                    });
                }
            } catch (IllegalArgumentException x) {
                log.warn(x.getMessage());
            }
        } else {
            log.warn("Discarding Record Job. Start time is in the past.");
        }
    }

    private void cancelRecordJobHandler(JsonObject event) {
        vertx.cancelTimer(timerId);

        JsonObject recordJob = RecrepJobRegistry.recordJobMap.get(event.getJsonObject(RecrepEventFields.PAYLOAD).getString(RecrepRecordJobFields.NAME));

        RecrepJobRegistry.unregisterRecordStreamHandler(recordJob.getString(RecrepRecordJobFields.NAME)).forEach(deploymentID -> {
            vertx.undeploy(deploymentID);
        });

        long now = System.currentTimeMillis();
        recordJob.put(RecrepRecordJobFields.TIMESTAMP_END, now);

        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_FINISHED, recordJob));
    }

    private void appendMetric(JsonObject recordJob, JsonObject metric) {
        recordJob.getJsonArray(RecrepRecordJobFields.SOURCE_MAPPINGS)
                .stream()
                .map( mapping -> (JsonObject) mapping )
                .filter( mapping -> mapping.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER)
                        .equals(metric.getString(RecrepJobMetricFields.ENDPOINT_IDENTIFIER)))
                .findFirst()
                .ifPresent( mapping -> {
                    mapping.put(RecrepEndpointMappingFields.METRICS, metric.getJsonObject(RecrepJobMetricFields.METRICS));
                } );
    }
}

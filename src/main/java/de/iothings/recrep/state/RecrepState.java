package de.iothings.recrep.state;

import de.iothings.recrep.common.JobConfigHelper;
import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Created by ue60219 on 21.02.2017.
 */
public class RecrepState extends AbstractVerticle {

    private RecrepLogHelper log;
    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    // PubSub Handler
    private final Handler<JsonObject> recrepJobInventoryUpdateHandler = this::handleJobInventoryUpdate;
    private final Handler<JsonObject> configurationStreamHandler = this::handleConfigurationStream;

    // Command Message Handler
    private final Handler<Message<JsonObject>> recrepStateRequestHandler = this::handleStateRequest;
    private final Handler<Message<JsonObject>> recrepConfigurationRequestHandler = this::handleConfigurationRequest;

    private ConfigRetriever configRetriever;

    private HashMap<String, JsonObject> recordJobs = new HashMap<>();
    private HashMap<String, JsonObject> scheduledRecordJobs = new HashMap<>();
    private HashMap<String, JsonObject> runningRecordJobs = new HashMap<>();

    private HashMap<String, JsonObject> scheduledReplayJobs = new HashMap<>();
    private HashMap<String, JsonObject> runningReplayJobs = new HashMap<>();

    @Override
    public void start() throws Exception {
        log = new RecrepLogHelper(vertx, RecrepState.class.getName());
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        subscribeToReqrepEvents();
        listenForRecrepCommands();
        initializeConfiguration();
        log.info("Started " + this.getClass().getName());
    }

    private void initializeConfiguration() {
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setConfig(new JsonObject().put("path", "./recrep-config.json"));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(fileStore);

        configRetriever = ConfigRetriever.create(vertx, options);

        configRetriever.getConfig(config -> {
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.CONFIGURATION_UPDATE,
                    config.result()));
        });

        configRetriever.listen(change -> {
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.CONFIGURATION_UPDATE,
                    change.getNewConfiguration()));
        });

    }

    private void listenForRecrepCommands() {
        vertx.eventBus().consumer(EventBusAddress.STATE_REQUEST.toString(), recrepStateRequestHandler);
        vertx.eventBus().consumer(EventBusAddress.CONFIGURATION_REQUEST.toString(), recrepConfigurationRequestHandler);
    }

    private void subscribeToReqrepEvents() {
        eventSubscriber.subscribe(recrepJobInventoryUpdateHandler, RecrepEventType.RECORDJOB_INVENTORY);
        eventSubscriber.subscribe(configurationStreamHandler, RecrepEventType.CONFIGURATION_UPDATE);
    }

    private void handleJobInventoryUpdate(JsonObject update) {
        log.info("Received state update: " + update.toString());
        update.getJsonObject(RecrepEventFields.PAYLOAD).getJsonArray("recordJobs").forEach(recordJob -> {
            recordJobs.put(((JsonObject) recordJob).getString(RecrepRecordJobFields.NAME), (JsonObject) recordJob);
        } );
        log.info("State after state update: " + createStateSnapshot());
    }

    private void handleConfigurationStream(JsonObject event) {
        JsonObject configuration = event.getJsonObject(RecrepEventFields.PAYLOAD);
        ArrayList<String> diretories = new ArrayList<>(configuration.getJsonObject(RecrepConfigurationFields.INVENTORY)
                .getJsonArray(RecrepConfigurationFields.INVENTORY_DIRECTORIES).getList());
        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_INVENTORY,
                new JsonObject().put("recordJobs", new JsonArray(JobConfigHelper.getJobConfigStream(diretories).collect(Collectors.toList())))));
    }


    // Command Handler
    private void handleStateRequest(Message stateRequest) {
        stateRequest.reply(createStateSnapshot());
    }

    private void handleConfigurationRequest(Message stateRequest) {
        configRetriever.getConfig(configResult -> {
            stateRequest.reply(configResult.result());
        });
    }

    private JsonObject createStateSnapshot() {
        return new JsonObject()
            .put("recordJobs", new ArrayList<>(recordJobs.values()))
            .put("scheduledRecordJobs",new ArrayList<>(scheduledRecordJobs.values()))
            .put("runningRecordJobs", new ArrayList<>(runningRecordJobs.values()))
            .put("scheduledReplayJobs", new ArrayList<>(scheduledReplayJobs.values()))
            .put("runningReplayJobs", new ArrayList<>(runningReplayJobs.values()))
            .put("recrepConfiguration", configRetriever.getCachedConfig());
    }


}

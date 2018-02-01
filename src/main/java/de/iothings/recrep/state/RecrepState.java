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
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ue60219 on 21.02.2017.
 */
public class RecrepState extends AbstractVerticle {

    private RecrepLogHelper log;
    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    // PubSub Handler
    private final Handler<JsonObject> configurationStreamHandler = this::handleConfigurationStream;
    private final Handler<JsonObject> recordJobUpdateHandler = this::handleRecordJobEvent;
    private final Handler<JsonObject> replayJobUpdateHandler = this::handleReplayJobEvent;

    // Command Message Handler
    private final Handler<Message<JsonObject>> recrepStateRequestHandler = this::handleStateRequest;
    private final Handler<Message<JsonObject>> recrepConfigurationRequestHandler = this::handleConfigurationRequest;

    private ConfigRetriever configRetriever;

    // Job Inventory Maps
    private HashMap<String, JsonObject> recordJobs = new HashMap<>();
    private HashMap<String, JsonObject> scheduledRecordJobs = new HashMap<>();
    private HashMap<String, JsonObject> activeRecordJobs = new HashMap<>();

    private HashMap<String, JsonObject> scheduledReplayJobs = new HashMap<>();
    private HashMap<String, JsonObject> activeReplayJobs = new HashMap<>();

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
        eventSubscriber.subscribe(configurationStreamHandler, RecrepEventType.CONFIGURATION_UPDATE);
        eventSubscriber.subscribe(recordJobUpdateHandler, RecrepEventType.RECORDJOB_REQUEST,
                RecrepEventType.RECORDJOB_STARTED, RecrepEventType.RECORDJOB_FINISHED, RecrepEventType.RECORDJOB_CANCEL_REQUEST, RecrepEventType.RECORDJOB_DELETE_REQUEST);
        eventSubscriber.subscribe(replayJobUpdateHandler, RecrepEventType.REPLAYJOB_REQUEST,
                RecrepEventType.REPLAYJOB_STARTED, RecrepEventType.REPLAYJOB_FINISHED, RecrepEventType.REPLAYJOB_CANCEL_REQUEST);
    }

    private void handleConfigurationStream(JsonObject event) {
        JsonObject configuration = event.getJsonObject(RecrepEventFields.PAYLOAD);
        ArrayList<String> diretories = new ArrayList<>(configuration.getJsonObject(RecrepConfigurationFields.INVENTORY)
                .getJsonArray(RecrepConfigurationFields.INVENTORY_DIRECTORIES).getList());

        JobConfigHelper.getJobConfigStream(diretories).forEach(recordJob -> {
            recordJobs.put(recordJob.getString(RecrepRecordJobFields.NAME), recordJob);
        });

        publishStateSnapshot();
    }

    // specific job handler

    private void handleRecordJobEvent(JsonObject event) {
         JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);

         switch (RecrepEventType.valueOf(event.getString(RecrepEventFields.TYPE))) {

             case RECORDJOB_REQUEST:
                 //add to scheduled record jobs
                 scheduledRecordJobs.put(recordJob.getString(RecrepRecordJobFields.NAME), recordJob);
                 publishStateSnapshot();
                 break;
             case RECORDJOB_STARTED:
                 //remove from scheduled record jobs and add to running jobs
                 scheduledRecordJobs.remove(recordJob.getString(RecrepRecordJobFields.NAME));
                 activeRecordJobs.put(recordJob.getString(RecrepRecordJobFields.NAME), recordJob);
                 publishStateSnapshot();
                 break;
             case RECORDJOB_FINISHED:
                 //remove from running record jobs and add to job inventory
                 activeRecordJobs.remove(recordJob.getString(RecrepRecordJobFields.NAME));
                 recordJobs.put(recordJob.getString(RecrepRecordJobFields.NAME), recordJob);
                 publishStateSnapshot();
                 break;
             case RECORDJOB_CANCEL_REQUEST:
                 //remove from running record jobs and add to job inventory
                 scheduledRecordJobs.remove(recordJob.getString(RecrepRecordJobFields.NAME));
                 activeRecordJobs.remove(recordJob.getString(RecrepRecordJobFields.NAME));
                 recordJobs.put(recordJob.getString(RecrepRecordJobFields.NAME), recordJob);
                 publishStateSnapshot();
                 break;
             case RECORDJOB_DELETE_REQUEST:
                 //remove from record jobs
                 recordJobs.remove(recordJob.getString(RecrepRecordJobFields.NAME));
                 publishStateSnapshot();
                 break;
             default:
                 //do nothing
         }
    }

    private void handleReplayJobEvent(JsonObject event) {
        JsonObject replayJob = event.getJsonObject(RecrepEventFields.PAYLOAD);

        switch (RecrepEventType.valueOf(event.getString(RecrepEventFields.TYPE))) {
            case REPLAYJOB_REQUEST:
                //add to scheduled record jobs
                scheduledReplayJobs.put(replayJob.getString(RecrepRecordJobFields.NAME), replayJob);
                publishStateSnapshot();
                break;
            case REPLAYJOB_STARTED:
                //remove from scheduled record jobs and add to running jobs
                scheduledReplayJobs.remove(replayJob.getString(RecrepRecordJobFields.NAME));
                activeReplayJobs.put(replayJob.getString(RecrepRecordJobFields.NAME), replayJob);
                publishStateSnapshot();
                break;
            case REPLAYJOB_FINISHED:
                //remove from running record jobs and add to job inventory
                activeReplayJobs.remove(replayJob.getString(RecrepRecordJobFields.NAME));
                publishStateSnapshot();
                break;
            case REPLAYJOB_CANCEL_REQUEST:
                //remove from running record jobs and add to job inventory
                scheduledReplayJobs.remove(replayJob.getString(RecrepRecordJobFields.NAME));
                activeReplayJobs.remove(replayJob.getString(RecrepRecordJobFields.NAME));
                publishStateSnapshot();
                break;
            default:
                //do nothing
        }
    }

    // Command Handler
    private void handleStateRequest(Message stateRequest) {
        stateRequest.reply(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_INVENTORY, createStateSnapshot()));
    }

    private void handleConfigurationRequest(Message stateRequest) {
        configRetriever.getConfig(configResult -> {
            stateRequest.reply(configResult.result());
        });
    }

    private void publishStateSnapshot() {
        if(eventPublisher != null) {
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_INVENTORY, createStateSnapshot()));
        }
    }

    private JsonObject createStateSnapshot() {
        return new JsonObject()
            .put("recordJobs", new ArrayList<>(recordJobs.values()))
            .put("scheduledRecordJobs",new ArrayList<>(scheduledRecordJobs.values()))
            .put("activeRecordJobs", new ArrayList<>(activeRecordJobs.values()))
            .put("scheduledReplayJobs", new ArrayList<>(scheduledReplayJobs.values()))
            .put("activeReplayJobs", new ArrayList<>(activeReplayJobs.values()))
            .put("recrepConfiguration", configRetriever.getCachedConfig());
    }
}

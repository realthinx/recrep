package de.iothings.recrep.state;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.EventBusAddress;
import de.iothings.recrep.model.RecrepEventType;
import de.iothings.recrep.model.RecrepRecordJobFields;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ue60219 on 21.02.2017.
 */
public class RecrepStore {

    private RecrepLogHelper log;
    private Vertx vertx;
    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    private final Handler<JsonObject> recrepInventoryUpdateHandler = this::handleInventoryUpdate;
    private final Handler<JsonObject> recrepConfigurationUpdateHandler = this::handleConfigurationUpdate;
    private final Handler<Message<JsonObject>> recrepStateRequestHandler = this::handleStateRequest;

    private JsonObject recrepConfiguration = new JsonObject();

    private HashMap<String, JsonObject> recordJobs = new HashMap<>();
    private HashMap<String, JsonObject> scheduledRecordJobs = new HashMap<>();
    private HashMap<String, JsonObject> runningRecordJobs = new HashMap<>();

    private HashMap<String, JsonObject> scheduledReplayJobs = new HashMap<>();
    private HashMap<String, JsonObject> runningReplayJobs = new HashMap<>();


    public RecrepStore(Vertx vertx) {
        this.vertx = vertx;
        this.log = new RecrepLogHelper(vertx, RecrepStore.class.getName());
        this.eventPublisher = new EventPublisher(vertx);
        this.eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        this.eventSubscriber.subscribe(recrepInventoryUpdateHandler, RecrepEventType.STATE_UPDATE);
        this.eventSubscriber.subscribe(recrepConfigurationUpdateHandler, RecrepEventType.CONFIGURATION_UPDATE);
        this.vertx.eventBus().consumer(EventBusAddress.STATE_REQUEST.toString(), recrepStateRequestHandler);
    }

    private void handleInventoryUpdate(JsonObject update) {
        String type = update.getJsonObject("payload").getString("action");

        switch (type) {
            case "RECORDJOB_INVENTORY":
                log.info("Received State update: " + update.toString());
                update.getJsonObject("payload").getJsonArray("payload").forEach( recordJob -> {
                    recordJobs.put(((JsonObject) recordJob).getString(RecrepRecordJobFields.NAME), (JsonObject) recordJob);
                } );
        }

        log.info("State after State update: " + createStateSnapshot());
    }

    private void handleConfigurationUpdate(JsonObject update) {
        log.info("Received Configuration update: " + update.toString());
        recrepConfiguration = update.getJsonObject("payload");
        log.info("State after Configuration update: " + createStateSnapshot());
    }

    private void handleStateRequest(Message statRequest) {
        statRequest.reply(createStateSnapshot());
    }

    private JsonObject createStateSnapshot() {
        return new JsonObject()
            .put("recordJobs", new ArrayList<JsonObject>(recordJobs.values()))
            .put("scheduledRecordJobs",new ArrayList<JsonObject>(scheduledRecordJobs.values()))
            .put("runningRecordJobs", new ArrayList<JsonObject>(runningRecordJobs.values()))
            .put("scheduledReplayJobs", new ArrayList<JsonObject>(scheduledReplayJobs.values()))
            .put("runningReplayJobs", new ArrayList<JsonObject>(runningReplayJobs.values()))
            .put("recrepConfiguration", recrepConfiguration);
    }


}

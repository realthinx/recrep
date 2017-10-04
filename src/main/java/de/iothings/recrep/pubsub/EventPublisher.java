package de.iothings.recrep.pubsub;

import de.iothings.recrep.Recorder;
import de.iothings.recrep.RecrepEngine;
import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.EventBusAddress;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by johannes on 24.12.16.
 */
public class EventPublisher {

    private RecrepLogHelper log;
    private Vertx vertx;

    public EventPublisher(Vertx vertx) {
        this.vertx = vertx;
        this.log = new RecrepLogHelper(vertx, EventPublisher.class.getName());
    }

    public void publish(JsonObject recrepEvent) {
        vertx.eventBus().publish(EventBusAddress.RECREP_EVENTS.toString(), recrepEvent);
        log.debug("Published to: " + EventBusAddress.RECREP_EVENTS.toString() + " - Event: " + recrepEvent);
    }

}

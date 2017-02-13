package de.iothings.recrep.pubsub;

import de.iothings.recrep.model.EventBusAddress;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Created by ue60219 on 13.02.2017.
 */
public class LogPublisher {

    private Vertx vertx;

    public LogPublisher(Vertx vertx) {
        this.vertx = vertx;
    }

    public void publish(JsonObject logEvent) {
        vertx.eventBus().publish(EventBusAddress.RECREP_LOGSTREAM.toString(), logEvent);
    }
}

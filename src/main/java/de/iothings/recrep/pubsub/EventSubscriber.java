package de.iothings.recrep.pubsub;

import de.iothings.recrep.model.RecrepEventFields;
import de.iothings.recrep.model.RecrepEventType;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by johannes on 26.12.16.
 */
public class EventSubscriber {

    private final Logger log = LoggerFactory.getLogger(EventSubscriber.class.getName());
    private Vertx vertx;
    private String address;

    public EventSubscriber(Vertx vertx, String address) {
        this.vertx = vertx;
        this.address = address;
    }

    public void subscribe(Handler<JsonObject> handler, RecrepEventType... eventTypes) {

        List<RecrepEventType> typesFilterList = Arrays.asList(eventTypes);

        vertx.eventBus().<JsonObject>consumer(address, message -> {
            RecrepEventType type = Enum.valueOf(RecrepEventType.class, message.body().getString(RecrepEventFields.TYPE));
            if (typesFilterList.contains(type)) {
                log.debug("Deliver Event type " + type + " payload: " + message.body());
                handler.handle(message.body());
            }
        });

        log.debug("Created subscription to address: " + address + " - Event types filter: " + typesFilterList);
    }

}

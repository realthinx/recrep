package de.iothings.recrep.pubsub;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.RecrepEventFields;
import de.iothings.recrep.model.RecrepEventType;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by johannes on 26.12.16.
 */
public class EventSubscriber {

    private RecrepLogHelper log;
    private Vertx vertx;
    private String address;

    public EventSubscriber(Vertx vertx, String address) {
        this.vertx = vertx;
        this.address = address;
        this.log = new RecrepLogHelper(vertx, EventSubscriber.class.getName());
    }

    public MessageConsumer subscribe(Handler<JsonObject> handler, RecrepEventType... eventTypes) {

        List<RecrepEventType> typesFilterList = Arrays.asList(eventTypes);

        MessageConsumer messageConsumer = vertx.eventBus().<JsonObject>consumer(address, message -> {
            //log.info("address: " + address + " got message: " + message.body());
            RecrepEventType type = Enum.valueOf(RecrepEventType.class, message.body().getString(RecrepEventFields.TYPE));
            if (typesFilterList.contains(type)) {
                log.info("Deliver Event type " + type + " payload: " + message.body());
                handler.handle(message.body());
            }
        });

        log.info("Created subscription to address: " + address + " - Event types filter: " + typesFilterList);

        return messageConsumer;
    }

}

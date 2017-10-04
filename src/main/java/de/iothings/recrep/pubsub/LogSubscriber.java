package de.iothings.recrep.pubsub;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.*;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.List;

/**
 * Created by johannes on 26.12.16.
 */
public class LogSubscriber {

    private RecrepLogHelper log;
    private Vertx vertx;

    public LogSubscriber(Vertx vertx) {
        this.vertx = vertx;
        this.log = new RecrepLogHelper(vertx, LogSubscriber.class.getName());
    }

    public MessageConsumer subscribe(Handler<JsonObject> handler, RecrepLogLevel... levels) {

        List<RecrepLogLevel> levelList = Arrays.asList(levels);

        MessageConsumer messageConsumer = vertx.eventBus().<JsonObject>consumer(EventBusAddress.RECREP_LOGSTREAM.toString(), message -> {
            RecrepLogLevel type = Enum.valueOf(RecrepLogLevel.class, message.body().getString(RecrepLogEventFields.LEVEL));
            if (levelList.contains(type)) {
                handler.handle(message.body());
            }
        });

        log.debug("Created subscription to address: " + EventBusAddress.RECREP_LOGSTREAM.toString() + " - Log levels filter: " + levelList);

        return messageConsumer;
    }

}

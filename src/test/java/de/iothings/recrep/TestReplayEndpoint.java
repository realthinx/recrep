package de.iothings.recrep;

import de.iothings.recrep.model.RecrepEndpointMappingFields;
import de.iothings.recrep.model.RecrepReplayJobFields;
import de.iothings.recrep.model.RecrepSignalType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;

/**
 * Created by johannes on 17.03.17.
 */
public class TestReplayEndpoint extends AbstractVerticle {

    @Override
    public void start() {

        System.out.println("Deployed Test Replay Endpoint: " + this.getClass().getName() + " - " + config().toString());

        String eventBusAddress = config().getString(RecrepReplayJobFields.NAME) + "_" + config().getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER);
        vertx.eventBus().consumer(eventBusAddress).handler(this::handleMessage);

        //Publish Replay Endpoint Ready
        vertx.eventBus().publish(eventBusAddress + "_" + RecrepSignalType.REPLAYENDPOINT_READY,"");
    }

    @Override
    public void stop() {
        System.out.println("Undeployed Test Replay Endpoint: " + this.getClass().getName() + " - " + config().toString());
    }

    private void handleMessage(Message message) {
        System.out.println(System.currentTimeMillis() + "|" + this.getClass().getName() + "|Received Replay Message: " + message.body().toString() + " |" + config().getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER));
    }

}

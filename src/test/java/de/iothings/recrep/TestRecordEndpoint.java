package de.iothings.recrep;

import de.iothings.recrep.model.RecrepEndpointMappingFields;
import de.iothings.recrep.model.RecrepRecordJobFields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

/**
 * Created by johannes on 17.03.17.
 */
public class TestRecordEndpoint extends AbstractVerticle {

    private long timerId;

    @Override
    public void start() {

        String eventBusAddress = config().getString(RecrepRecordJobFields.NAME);
        String stage = config().getString(RecrepEndpointMappingFields.STAGE);
        String sourceIdentifier = config().getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER);
        Long interval = config().getJsonObject(RecrepEndpointMappingFields.PROPERTIES).getLong("interval");

        System.out.println("Deployed Test Record Endpoint: " + this.getClass().getName() + " - " + config().toString());

        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("source", sourceIdentifier);

        timerId = vertx.setPeriodic(interval, tick -> {
            JsonObject jsonObject = new JsonObject()
                    .put("index",tick)
                    .put("payload",new String(UUID.randomUUID().toString()))
                    .put("source", sourceIdentifier);
            vertx.eventBus().publish(eventBusAddress, jsonObject, deliveryOptions);
        });
    }

    @Override
    public void stop() {
        vertx.cancelTimer(timerId);
        System.out.println("Undeployed Test Record Endpoint: " + this.getClass().getName() + " - " + config().toString());
    }

}

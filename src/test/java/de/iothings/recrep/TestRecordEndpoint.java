package de.iothings.recrep;

import de.iothings.recrep.model.RecrepEndpointMappingFields;
import de.iothings.recrep.model.RecrepRecordJobFields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by johannes on 17.03.17.
 */
public class TestRecordEndpoint extends AbstractVerticle {

    private long timerId;

    @Override
    public void start() {

        ArrayList<String> randomWords = new ArrayList<>(Arrays.asList(
                "Maus", "Haus", "Auto", "Blume", "Hund", "Strasse", "Berg"
        ));

        String eventBusAddress = config().getString(RecrepRecordJobFields.NAME);
        String stage = config().getString(RecrepEndpointMappingFields.STAGE);
        String sourceIdentifier = config().getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER);
        Long interval = config().getJsonObject(RecrepEndpointMappingFields.PROPERTIES).getLong("interval");

        System.out.println("Deployed Test Record Endpoint: " + this.getClass().getName() + " - " + config().toString());

        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("source", sourceIdentifier);

        timerId = vertx.setPeriodic(interval, tick -> {
            int randomNum1 = ThreadLocalRandom.current().nextInt(0, 6 + 1);
            int randomNum2 = ThreadLocalRandom.current().nextInt(0, 6 + 1);
            JsonObject jsonObject = new JsonObject()
                    .put("index",tick)
                    // .put("payload",new String(UUID.randomUUID().toString()))
                    .put("payload", randomWords.get(randomNum1) + " " + randomWords.get(randomNum2))
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

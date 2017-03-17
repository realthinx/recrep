package de.iothings.recrep;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Created by johannes on 10.03.17.
 */
public class TestLauncher {

    public static void main(String[] args) {

        Vertx vertx = Vertx.vertx();


        DeploymentOptions testRecordOptions1 = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put("eventBusAddress", "teststream_in_1")
                        .put("interval",1200l));

        DeploymentOptions testRecordOptions2 = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put("eventBusAddress", "teststream_in_2")
                        .put("interval",2000l));

        vertx.deployVerticle("de.iothings.recrep.TestRecordEndpoint", testRecordOptions1);
        vertx.deployVerticle("de.iothings.recrep.TestRecordEndpoint", testRecordOptions2);

        DeploymentOptions testReplayOptions = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put("eventBusAddress", "teststream_out"));

        vertx.deployVerticle("de.iothings.recrep.TestReplayEndpoint", testReplayOptions);

        List<String> deployableList = Collections.emptyList();
        RecrepEmbedded recrepEmbedded = new RecrepEmbedded(vertx, deployableList);
        recrepEmbedded.deploy(finished -> {
            System.out.println("Deployment finished");
        });
    }

}

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

        List<String> deployableList = Collections.emptyList();
        RecrepEmbedded recrepEmbedded = new RecrepEmbedded(vertx, deployableList);
        recrepEmbedded.deploy(finished -> {
            System.out.println("Deployment finished");
        });
    }

}

package de.iothings.recrep;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ue60219 on 31.01.2017.
 */
public class RecrepRunner {

    private static Logger log = LoggerFactory.getLogger(RecrepRunner.class.getName());
    private static Vertx vertx;

    public static void main(String[] args) {
        vertx = Vertx.vertx();

        List<String> deployableList = Arrays.asList(RecrepApi.class.getName());
        RecrepEmbedded recrepEmbedded = new RecrepEmbedded(vertx, deployableList);
        recrepEmbedded.deploy(finished -> {
            log.info("Starting Test Data Job...");
            RecrepDemoRunner.runDemo(vertx);
        });

    }


}

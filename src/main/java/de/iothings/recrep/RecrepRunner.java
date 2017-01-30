package de.iothings.recrep;

import de.iothings.recrep.pubsub.EventPublisher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;


/**
 * Created by johannes on 10.12.16.
 */
public class RecrepRunner {

    private static Logger log = LoggerFactory.getLogger(RecrepRunner.class.getName());
    private static Vertx vertx;

    public static void main(String[] args) {
        //System.setProperty("vertx.cwd", "src/main/java/de/iothings/recrep");
        vertx = Vertx.vertx();

        RecrepEmbedded recrepEmbedded = new RecrepEmbedded(vertx);
        List<String> deployableList = Arrays.asList(RecrepApi.class.getName());
        recrepEmbedded.addOrderedDeployables(deployableList);
        recrepEmbedded.deploy(finished -> log.info("DONE."));

    }


}

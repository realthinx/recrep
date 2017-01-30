package de.iothings.recrep;

import de.iothings.recrep.pubsub.EventPublisher;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by johannes on 30.01.2017.
 */
public class RecrepEmbedded {

    private static Logger log = LoggerFactory.getLogger(RecrepEmbedded.class.getName());
    private static Vertx vertx;
    private static EventPublisher eventPublisher;
    private int deploymentIndex = 0;
    private VoidHandler deployFinishedHandler;

    private ArrayList<String> orderedDeployables;

    public RecrepEmbedded(Vertx vertx) {
        this.vertx = vertx;
        this.orderedDeployables = new ArrayList<>(Arrays.asList(
                RecrepEngine.class.getName(),
                Replayer.class.getName(),
                Recorder.class.getName(),
                RecrepApi.class.getName()));
    }

    public void addOrderedDeployables(List<String> orderedDeployables ) {
        this.orderedDeployables.addAll(orderedDeployables);
    }

    public void deploy(Handler<AsyncResultHandler<Void>> deploymentFinished) {
        deployNext(deploymentFinished);
    }

    private void deployNext(Handler<AsyncResultHandler<Void>> deploymentFinished) {
        Handler<AsyncResult<String>> deployNextHandler = stringAsyncResult -> deployNext(deploymentFinished);
        if(orderedDeployables.size() > deploymentIndex) {
            log.info("Deploying " + orderedDeployables.get(deploymentIndex));
            vertx.deployVerticle(orderedDeployables.get(deploymentIndex++), deployNextHandler);
        } else {
            log.info("Finished deployments");
            if(deploymentFinished != null) {
                deploymentFinished.handle(null);
            }
        }
    }
}

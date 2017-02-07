package de.iothings.recrep;

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
    private int deploymentIndex = 0;

    private ArrayList<String> orderedDeployables;

    public RecrepEmbedded(Vertx vertx, List<String> orderedDeployables) {

        this.vertx = vertx;

        this.orderedDeployables = new ArrayList<>(Arrays.asList(
                RecrepEngine.class.getName(),
                Replayer.class.getName(),
                Recorder.class.getName()
                //Api
                ));

        this.orderedDeployables.addAll(orderedDeployables);
    }

    public void deploy(Handler<AsyncResultHandler<Void>> deploymentFinished) {
        deployNext(deploymentFinished);
    }

    public void deploy() {
        deployNext(null);
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

package de.iothings.recrep;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.state.RecrepState;
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

    private static RecrepLogHelper log;
    private static Vertx vertx;
    private int deploymentIndex = 0;

    private ArrayList<String> orderedDeployables;

    public RecrepEmbedded(Vertx vertx, List<String> orderedDeployables) {

        this.vertx = vertx;
        this.log = new RecrepLogHelper(vertx, RecrepEmbedded.class.getName());

        this.orderedDeployables = new ArrayList<>(Arrays.asList(
                RecrepDefaultLogger.class.getName(),
                RecrepState.class.getName(),
                RecrepEngine.class.getName(),
                Replayer.class.getName(),
                Recorder.class.getName(),
                RecrepApi.class.getName()));

        this.orderedDeployables.addAll(orderedDeployables);
    }

    public void deploy(Handler<Void> deploymentFinished) {
        deployNext(deploymentFinished);
    }

    private void deployNext(Handler<Void> deploymentFinished) {
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

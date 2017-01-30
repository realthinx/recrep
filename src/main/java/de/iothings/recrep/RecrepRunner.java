package de.iothings.recrep;

import de.iothings.recrep.pubsub.EventPublisher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Created by johannes on 10.12.16.
 */
public class RecrepRunner {

    private static Logger log = LoggerFactory.getLogger(RecrepRunner.class.getName());
    private static Vertx vertx;
    private static EventPublisher eventPublisher;
    private static int deploymentIndex = 0;
    private static Handler<AsyncResult<String>> deployNextHandler = stringAsyncResult -> deployNext();

    private static String verticleDeploymentOrder[] = {
            //RecrepEngine.class.getName(),
            //Replayer.class.getName(),
            //Recorder.class.getName(),
            RecrepApi.class.getName()
    };

    public static void main(String[] args) {

        System.setProperty("vertx.cwd", "src/main/java/de/iothings/recrep");

        vertx = Vertx.vertx();
        eventPublisher = new EventPublisher(vertx);
        deployNext();
    }

    private static void deployNext() {
        if(verticleDeploymentOrder.length > deploymentIndex) {
            log.info("Deploying " + verticleDeploymentOrder[deploymentIndex]);
            vertx.deployVerticle(verticleDeploymentOrder[deploymentIndex++], deployNextHandler);
        } else {
            log.info("Finished deployments");
            //RecrepDemoRunner.runDemo(vertx);
        }
    }

}

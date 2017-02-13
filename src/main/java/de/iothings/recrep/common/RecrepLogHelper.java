package de.iothings.recrep.common;

import de.iothings.recrep.model.RecrepLogEventBuilder;
import de.iothings.recrep.model.RecrepLogLevel;
import de.iothings.recrep.pubsub.LogPublisher;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;

/**
 * Created by ue60219 on 13.02.2017.
 */
public class RecrepLogHelper {

    private String sourceIdentifier;
    private LogPublisher logPublisher;

    public RecrepLogHelper(Vertx vertx, String sourceIdentifier) {
       this.logPublisher = new LogPublisher(vertx);
       this.sourceIdentifier = sourceIdentifier;
    }


    public void debug(String message) {
        publishLogEvent(RecrepLogLevel.DEBUG, message);
    }

    public void info(String message) {
        publishLogEvent(RecrepLogLevel.INFO, message);
    }

    public void warn(String message) {
        publishLogEvent(RecrepLogLevel.WARNING, message);
    }

    public void error(String message) {
        publishLogEvent(RecrepLogLevel.ERROR, message);
    }

    public void error(String message, Throwable cause) {
        publishLogEvent(RecrepLogLevel.ERROR, message, cause);
    }

    private void publishLogEvent(RecrepLogLevel level, String message) {
        logPublisher.publish(RecrepLogEventBuilder.createLogEvent(level, message, sourceIdentifier));
    }

    private void publishLogEvent(RecrepLogLevel level, String message, Throwable cause) {

        JsonArray causeJson = new JsonArray();
        Arrays.asList(cause.getStackTrace()).forEach( stackTraceElement ->  {
            causeJson.add(stackTraceElement);
        });

        logPublisher.publish(RecrepLogEventBuilder.createLogEvent(level, message, sourceIdentifier, causeJson));
    }


}

package de.iothings.recrep.model;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by ue60219 on 13.02.2017.
 */
public class RecrepLogEventBuilder{

    private JsonObject event;

    public RecrepLogEventBuilder() {
        this.event = new JsonObject();
    }

    public static JsonObject createLogEvent(RecrepLogLevel level, String message, String sourceIdentifier) {
        return new RecrepLogEventBuilder()
                .withLevel(level)
                .withSourceIdentifier(sourceIdentifier)
                .withMessage(message)
                .build();
    }

    public static JsonObject createLogEvent(RecrepLogLevel level, String message, String sourceIdentifier, JsonArray cause) {
        return new RecrepLogEventBuilder()
                .withLevel(level)
                .withSourceIdentifier(sourceIdentifier)
                .withMessage(message)
                .withCause(cause)
                .build();
    }

    public RecrepLogEventBuilder withLevel(RecrepLogLevel level) {
        this.event.put(RecrepLogEventFields.LEVEL, level);
        return this;
    }

    public RecrepLogEventBuilder withMessage(String message) {
        this.event.put(RecrepLogEventFields.MESSAGE, message);
        return this;
    }

    public RecrepLogEventBuilder withSourceIdentifier(String sourceIdentifier) {
        this.event.put(RecrepLogEventFields.SOURCE_IDENTIFIER, sourceIdentifier);
        return this;
    }

    public RecrepLogEventBuilder withCause(JsonArray cause) {
        this.event.put(RecrepLogEventFields.CAUSE, cause);
        return this;
    }

    public JsonObject build() {
        return this.event;
    }

}

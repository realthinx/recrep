package de.iothings.recrep.model;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by johannes on 24.12.16.
 */
public class RecrepEventBuilder {

    private JsonObject event;

    public RecrepEventBuilder() {
        this.event = new JsonObject();
    }

    public static JsonObject createEvent(RecrepEventType type, JsonObject payload) {
        return new RecrepEventBuilder()
                .withType(type)
                .withPayload(payload)
                .build();
    }

    public RecrepEventBuilder withType(RecrepEventType type) {
        this.event.put(RecrepEventFields.TYPE, type);
        return this;
    }

    public RecrepEventBuilder withPayload(Object payload) {
        this.event.put(RecrepEventFields.PAYLOAD, payload);
        return this;
    }

    public JsonObject build() {
        return this.event;
    }

}

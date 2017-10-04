package de.iothings.recrep.model;

import io.vertx.core.json.JsonObject;

/**
 * Created by johannes on 24.12.16.
 */
public class RecrepEndpointMappingBuilder {

    private JsonObject event;

    public RecrepEndpointMappingBuilder() {
        this.event = new JsonObject();
    }

    public static JsonObject createMapping(String handler, String stage, String sourceIdentifier, String targetIdentifier) {
        return new RecrepEndpointMappingBuilder()
                .withHandler(handler)
                .withStage(stage)
                .withSourceIdentifier(sourceIdentifier)
                .withTargetIdentifier(targetIdentifier)
                .build();
    }

    public RecrepEndpointMappingBuilder withHandler(String handler) {
        this.event.put(RecrepEndpointMappingFields.HANDLER, handler);
        return this;
    }

    public RecrepEndpointMappingBuilder withStage(String stage) {
        this.event.put(RecrepEndpointMappingFields.STAGE, stage);
        return this;
    }

    public RecrepEndpointMappingBuilder withSourceIdentifier(String sourceIdentifier) {
        this.event.put(RecrepEndpointMappingFields.SOURCE_IDENTIFIER, sourceIdentifier);
        return this;
    }

    public RecrepEndpointMappingBuilder withTargetIdentifier(String targetIdentifier) {
        this.event.put(RecrepEndpointMappingFields.TARGET_IDENTIFIER, targetIdentifier);
        return this;
    }

    public JsonObject build() {
        return this.event;
    }

}

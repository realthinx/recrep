package de.iothings.recrep.common;

import de.iothings.recrep.model.RecrepConfigurationFields;
import de.iothings.recrep.model.RecrepEndpointMappingFields;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

/**
 * Created by johannes on 18.03.17.
 */
public class EndpointConfigHelper {

    public static JsonObject lookupRecordHandlerProperties(JsonObject recrepConfiguration, String handler, String stage, String sourceIdentifier) {
        Optional<JsonObject> found = recrepConfiguration.getJsonArray(RecrepConfigurationFields.RECORD_ENDPOINTS)
                .stream()
                .map(element -> (JsonObject) element)
                .filter(recordEndpoint -> {
                    String recordEndpointHandler = recordEndpoint.getString(RecrepEndpointMappingFields.HANDLER);
                    String recordEndpointStage = recordEndpoint.getString(RecrepEndpointMappingFields.STAGE);
                    String recordEndpointSourceIdentifier = recordEndpoint.getString(RecrepEndpointMappingFields.SOURCE_IDENTIFIER);
                    return recordEndpointHandler.equals(handler)
                            && recordEndpointStage.equals(stage)
                            && recordEndpointSourceIdentifier.equals(sourceIdentifier);
                })
                .findFirst();

        if(found.isPresent()) {
            return found.get();
        } else {
            return null;
        }
    }

    public static JsonObject lookupReplayHandlerProperties(JsonObject recrepConfiguration, String handler, String stage, String targetIdentifier) {
        Optional<JsonObject> found = recrepConfiguration.getJsonArray(RecrepConfigurationFields.REPLAY_ENDPOINTS)
                .stream()
                .map(element -> (JsonObject) element)
                .filter( recordEndpoint -> {
                    String recordEndpointHandler = recordEndpoint.getString(RecrepEndpointMappingFields.HANDLER);
                    String recordEndpointStage = recordEndpoint.getString(RecrepEndpointMappingFields.STAGE);
                    String recordEndpointTargetIdentifier = recordEndpoint.getString(RecrepEndpointMappingFields.TARGET_IDENTIFIER);
                    return recordEndpointHandler.equals(handler)
                            && recordEndpointStage.equals(stage)
                            && recordEndpointTargetIdentifier.equals(targetIdentifier);
                })
                .findFirst();

        if(found.isPresent()) {
            return found.get();
        } else {
            return null;
        }
    }
}

package de.iothings.recrep;

import de.iothings.recrep.model.EventBusAddress;
import de.iothings.recrep.pubsub.EventSubscriber;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by johannes on 27.12.16.
 */
public class RecrepApi extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(RecrepApi.class.getName());

    private JsonObject recrepConfiguration;

    private EventSubscriber eventSubscriber = null;

    private HttpServer server = null;

    // todo: necessary?
    // private Handler<JsonObject> configurationUpdateHandler = this::handleConfigurationUpdate;

    @Override
    public void start() throws Exception {
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        // todo: necessary?
        subscribeToReqrepEvents();
        initializeConfiguration();
        log.info("Started " + this.getClass().getName());
    }

    @Override
    public void stop() throws Exception {
        if(server != null) {
            server.close();
        }
    }

    private void subscribeToReqrepEvents() {
        // todo: necessary?
        // eventSubscriber.subscribe(configurationUpdateHandler, RecrepEventType.CONFIGURATION_UPDATE);
    }

    private void initializeConfiguration() {
        vertx.eventBus().send(EventBusAddress.CONFIGURATION_REQUEST.toString(), new JsonObject(), configurationReply -> {
            recrepConfiguration = (JsonObject) configurationReply.result().body();
            if(server == null) {
                startServer();
            }
        });
    }

    /* todo: necessary?
    private void handleConfigurationUpdate(JsonObject event) {
        recrepConfiguration = event.getJsonObject(RecrepEventFields.PAYLOAD);
        if(server == null) {
            startServer();
        }
    }
    */

    private void startServer() {
        server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
        BridgeOptions options = new BridgeOptions();
        PermittedOptions inboundPermitted = new PermittedOptions().setAddressRegex(".*");
        PermittedOptions outboundPermitted = new PermittedOptions().setAddressRegex(".*");
        options
                .addInboundPermitted(inboundPermitted)
                .addOutboundPermitted(outboundPermitted);

        sockJSHandler.bridge(options);

        //bridge
        router.route("/eventbus/*").handler(sockJSHandler);

        // Serve the static resources
        router.route().handler(StaticHandler.create());

        server.requestHandler(router::accept).listen(recrepConfiguration.getInteger("web.port", 8082));
        log.info("Recrep API listens on port: " + server.actualPort());
    }
}

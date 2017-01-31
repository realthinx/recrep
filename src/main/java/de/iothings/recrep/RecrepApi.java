package de.iothings.recrep;

import de.iothings.recrep.model.EventBusAddress;
import de.iothings.recrep.model.RecrepEventBuilder;
import de.iothings.recrep.model.RecrepEventType;
import de.iothings.recrep.pubsub.EventPublisher;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeEventType;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by johannes on 27.12.16.
 */
public class RecrepApi extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(RecrepApi.class.getName());

    private static EventPublisher eventPublisher;

    @Override
    public void start() throws Exception {

        eventPublisher = new EventPublisher(vertx);

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
        BridgeOptions options = new BridgeOptions();
        PermittedOptions inboundPermitted = new PermittedOptions().setAddressRegex(".*");
        PermittedOptions outboundPermitted = new PermittedOptions().setAddressRegex(".*");
        options
            .addInboundPermitted(inboundPermitted)
            .addOutboundPermitted(outboundPermitted);
        //sockJSHandler.bridge(options);

        vertx.setPeriodic(1000, tick -> {
                    vertx.eventBus().publish("test","test");
                }
        );


//        router.route("/eventbus/*").handler(SockJSHandler.create(vertx).bridge(options, event -> {
//
//            if (event.type() == BridgeEventType.SOCKET_CREATED) {
//                //clientConnections.put(event.socket().writeHandlerID(), clientConnections.size()+1);
//                System.out.println("A socket was created " + event.socket().writeHandlerID());
//                vertx.eventBus().send(event.socket().writeHandlerID(),Buffer.buffer().appendString(event.socket().writeHandlerID()));
//
//                JsonObject jsonObject = new JsonObject().put("message","A new client joined");
//                vertx.setTimer(100, tick -> {
//                        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.TEST, jsonObject));
//                    }
//                );
//
//            }
//
//            if (event.type() == BridgeEventType.SOCKET_CLOSED) {
//                //clientConnections.remove(event.socket().writeHandlerID());
//                System.out.println("A socket was closed " + event.socket().writeHandlerID());
//            }
//
//            if (event.type() == BridgeEventType.PUBLISH) {
//                System.out.println("A message was published " + event.socket().writeHandlerID());
//            }
//
//            if (event.type() == BridgeEventType.RECEIVE) {
//                System.out.println("A message was received " + event.socket().writeHandlerID());
//            }
//
//            if (event.type() == BridgeEventType.SEND) {
//                System.out.println("A message was sent " + event.socket().writeHandlerID());
//            }
//
//            if (event.type() == BridgeEventType.REGISTER) {
//                System.out.println("A sub was registered " + event.socket().writeHandlerID());
//            }
//
//            if (event.type() == BridgeEventType.UNREGISTER) {
//                System.out.println("A sub was unregistered " + event.socket().writeHandlerID());
//            }
//
//            // This signals that it's ok to process the event
//            event.complete(true);
//
//        }));

        sockJSHandler.bridge(options, event -> {

            if (event.type() == BridgeEventType.SOCKET_CREATED) {
                //clientConnections.put(event.socket().writeHandlerID(), clientConnections.size()+1);
                System.out.println("A socket was created " + event.socket().writeHandlerID());
                //vertx.eventBus().send(event.socket().writeHandlerID(),Buffer.buffer().appendString(event.socket().writeHandlerID()));

                JsonObject jsonObject = new JsonObject().put("type","rec").put("address",event.socket().writeHandlerID()).put("body",new JsonObject().put("message","A new client joined"));

                event.socket().write(Buffer.buffer().appendString(jsonObject.toString()));

                //vertx.eventBus().send(event.socket().writeHandlerID(),jsonObject);


                vertx.setTimer(100, tick -> {
                        eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.TEST, jsonObject));
                    }
                );

            }

            if (event.type() == BridgeEventType.SOCKET_CLOSED) {
                //clientConnections.remove(event.socket().writeHandlerID());
                System.out.println("A socket was closed " + event.socket().writeHandlerID());
            }

            if (event.type() == BridgeEventType.PUBLISH) {
                System.out.println("A message was published " + event.socket().writeHandlerID());
            }

            if (event.type() == BridgeEventType.RECEIVE) {
                System.out.println("A message was received " + event.socket().writeHandlerID());
            }

            if (event.type() == BridgeEventType.SEND) {
                System.out.println("A message was sent " + event.socket().writeHandlerID());
            }

            if (event.type() == BridgeEventType.REGISTER) {
                System.out.println("A sub was registered " + event.socket().writeHandlerID());
            }

            if (event.type() == BridgeEventType.UNREGISTER) {
                System.out.println("A sub was unregistered " + event.socket().writeHandlerID());
            }

            // This signals that it's ok to process the event
            event.complete(true);

        });

        //bridge
        router.route("/eventbus/*").handler(sockJSHandler);

        // Serve the static resources
        router.route().handler(StaticHandler.create());



        server.requestHandler(router::accept).listen(8080);

        log.info("Started " + this.getClass().getName());
    }

    @Override
    public void stop() throws Exception {

    }
}

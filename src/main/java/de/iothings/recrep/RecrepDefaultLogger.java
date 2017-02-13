package de.iothings.recrep;

import de.iothings.recrep.model.RecrepEventType;
import de.iothings.recrep.model.RecrepLogEventFields;
import de.iothings.recrep.model.RecrepLogLevel;
import de.iothings.recrep.pubsub.LogSubscriber;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by ue60219 on 13.02.2017.
 */
public class RecrepDefaultLogger extends AbstractVerticle {

    private HashMap<String, Logger> loggerHashMap = new HashMap<>();
    private LogSubscriber logSubscriber;
    private List<MessageConsumer> messageConsumerList = new ArrayList<>();

    private final Handler<JsonObject> logEventHandler = this::handleLogEvent;

    @Override
    public void start() throws Exception {
        this.logSubscriber = new LogSubscriber(vertx);
        messageConsumerList.add(logSubscriber.subscribe(logEventHandler, RecrepLogLevel.DEBUG, RecrepLogLevel.INFO, RecrepLogLevel.WARNING, RecrepLogLevel.ERROR));
    }

    @Override
    public void stop() throws Exception {
        messageConsumerList.forEach(MessageConsumer::unregister);
    }

    private void handleLogEvent(JsonObject logEvent){
        Logger log = getOrCreateLogger(logEvent.getString(RecrepLogEventFields.SOURCE_IDENTIFIER));
        RecrepLogLevel type = Enum.valueOf(RecrepLogLevel.class, logEvent.getString(RecrepLogEventFields.LEVEL));
        String message = logEvent.getString(RecrepLogEventFields.MESSAGE);
        switch (type) {
            case DEBUG:
                log.debug(message);
                break;
            case INFO:
                log.info(message);
                break;
            case WARNING:
                log.warn(message);
                break;
            case ERROR:
                log.error(message);
                break;
            default:
                log.debug(message);
                break;
        }
    }

    private Logger getOrCreateLogger(String sourceIdentifier) {
        if(loggerHashMap.containsKey(sourceIdentifier)) {
            return loggerHashMap.get(sourceIdentifier);
        } else {
            Logger log = LoggerFactory.getLogger(sourceIdentifier);
            loggerHashMap.put(sourceIdentifier, log);
            return log;
        }
    }


}

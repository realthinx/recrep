package de.iothings.recrep;

import de.iothings.recrep.common.RecordLogHelper;
import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import de.iothings.recrep.stream.RecordReadStream;
import de.iothings.recrep.stream.TimelineWriteStream;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by johannes on 21.12.16.
 */
public class Replayer extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(Replayer.class.getName());

    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;
    private List<MessageConsumer> messageConsumerList = new ArrayList<>();

    private final Handler<JsonObject> startReplayJobHandler = event -> startReplayJob(event);
    private final Handler<Throwable> exceptionHandler = throwable -> log.error(throwable.getMessage());

    @Override
    public void start() throws Exception {
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        subscribeToReqrepEvents();
        log.info("Started " + this.getClass().getName());
    }

    @Override
    public void stop() throws Exception {
        messageConsumerList.forEach(MessageConsumer::unregister);
        log.info("Stopped " + this.getClass().getName());
    }

    private void subscribeToReqrepEvents() {
        messageConsumerList.add(eventSubscriber.subscribe(startReplayJobHandler, RecrepEventType.REPLAYSTREAM_CREATED));
    }

    private void startReplayJob(JsonObject event) {

        JsonObject replayJob = event.getJsonObject(RecrepEventFields.PAYLOAD);

        Stream<String> recordFileLines = RecordLogHelper.getRecordLogFileStream(replayJob);
        if(recordFileLines != null) {
            RecordReadStream<String> recordStream = new RecordReadStream<>(recordFileLines);
            recordStream.exceptionHandler(exceptionHandler);

            TimelineWriteStream timelineStream = new TimelineWriteStream(vertx, replayJob.getString(RecrepReplayJobFields.NAME), 1000, replayJob.getInteger(RecrepReplayJobFields.SPEEDFACTOR));
            timelineStream.exceptionHandler(exceptionHandler);
            timelineStream.endHandler( end -> eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYJOB_FINISHED, replayJob)));

            Pump pump = Pump.pump(recordStream, timelineStream);
            recordStream.endHandler(end -> {
                pump.stop();
                timelineStream.end();
            });

            pump.start();
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.REPLAYJOB_STARTED, replayJob));
        } else {
            log.error("Failed to load record job log file. Discarding replay job.");
        }
    }

}

package de.iothings.recrep.stream;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by johannes on 22.12.16.
 */
public class TimelineWriteStream implements WriteStream<String> {

    private static final Logger log = LoggerFactory.getLogger(TimelineWriteStream.class.getName());

    private Vertx vertx;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> drainHandler;
    private Handler<Void> endHandler;
    private MessageProducer<JsonObject> messageProducer;
    private Integer writeQueueMaxSize;
    private Integer speedFactor;

    private AtomicBoolean paused = new AtomicBoolean(false);
    private AtomicLong timestampOffset = null;
    private AtomicLong lastTimestamp = null;
    private AtomicLong lastSchedule = null;
    private AtomicInteger scheduledTimers = new AtomicInteger(0);

    public TimelineWriteStream(Vertx vertx, String address, int maxBufferedTimers, Integer speedFactor) {
        this.vertx = vertx;
        this.messageProducer = vertx.eventBus().sender(address);
        this.writeQueueMaxSize = maxBufferedTimers;
        this.speedFactor = speedFactor;
    }

    @Override
    public WriteStream<String> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    public WriteStream<String> endHandler(Handler<Void> handler) {
        this.endHandler = handler;
        return this;
    }

    @Override
    public WriteStream<String> write(String s) {
        log.debug("write() called");
        try {
            JsonObject message = parse(s);
            if(speedFactor == null) {
                log.debug("Speed factor is not set, message replayed without delay");
                messageProducer.send(message);
            } else {
                Long recordTimestamp = message.getLong("timestamp");
                Long nowTimestamp = System.currentTimeMillis();
                if(lastTimestamp == null) {
                    timestampOffset = new AtomicLong(nowTimestamp - recordTimestamp);
                    lastTimestamp = new AtomicLong(recordTimestamp);
                    lastSchedule = new AtomicLong(nowTimestamp);
                    messageProducer.send(message);
                } else {
                    Long schedule = getScheduleWithOffset(recordTimestamp);
                    lastSchedule.set(schedule);
                    Long delay = schedule - nowTimestamp;
                    if(delay >= 1) {
                        try {
                            vertx.setTimer(delay, tick -> {
                                messageProducer.send(message);
                                doResume();
                            });
                        } catch (IllegalArgumentException x) {
                            log.warn(x.getMessage());
                            doResume();
                        }
                    } else {
                        if(delay < 1) {
                            log.warn("Running " + delay + " ms behind scheduled timeline!");
                        }
                        messageProducer.send(message);
                        doResume();
                    }
                }
                lastTimestamp.set(recordTimestamp);
                doPause();
            }
        } catch (Throwable t) {
            doHandleException(t);
        }

        return this;
    }

    private Long getScheduleWithOffset(Long recordTimestamp) {
        Long schedule = 0l;
        if(this.speedFactor == 1) {
            schedule = recordTimestamp + timestampOffset.get();
        } else {
            schedule = lastSchedule.get() + ((recordTimestamp - lastTimestamp.get()) / this.speedFactor);
        }
        return schedule;
    }

    private JsonObject parse(String recordLine) {
        JsonObject jsonObject = new JsonObject();
        String[] recordLineChunks = recordLine.split("\\|");
        jsonObject.put("timestamp", Long.valueOf(recordLineChunks[0]));
        jsonObject.put("source", recordLineChunks[1]);
        jsonObject.put("payload", recordLineChunks[2]);
        return jsonObject;
    }

    private void doPause() {
        if(scheduledTimers.incrementAndGet() > this.writeQueueMaxSize) {
            paused.set(true);
        }
    }

    private void doResume() {
        if(scheduledTimers.decrementAndGet() < (this.writeQueueMaxSize * 0.8)) {
            paused.set(false);
            if (this.drainHandler != null) {
                this.drainHandler.handle(null);
            }
        }
    }

    private void doHandleException(Throwable t) {
        log.error(t.getMessage());
        if(this.exceptionHandler != null) {
            this.exceptionHandler.handle(t);
        }
    }

    @Override
    public void end() {
        log.debug("WriteStream end() called");
        Long delay = (lastSchedule.get() - System.currentTimeMillis());
        if(delay < 0) {
            delay = 100L;
        } else {
            delay += 100L;
        }
        try {
            vertx.setTimer(delay, tick -> {
                log.debug("closing messageproducer for: " + messageProducer.address());
                messageProducer.close();
                if(this.endHandler!= null) {
                    this.endHandler.handle(null);
                }
            });
        } catch (IllegalArgumentException x) {
            log.warn(x.getMessage());
            log.debug("closing messageproducer for: " + messageProducer.address());
            messageProducer.close();
            if(this.endHandler!= null) {
                this.endHandler.handle(null);
            }
        }
    }

    @Override
    public WriteStream<String> setWriteQueueMaxSize(int i) {
        this.writeQueueMaxSize = i;
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return paused.get();
    }

    @Override
    public WriteStream<String> drainHandler(Handler<Void> handler) {
        this.drainHandler = handler;
        return this;
    }
}

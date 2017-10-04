package de.iothings.recrep.stream;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Created by johannes on 22.12.16.
 */
public class RecordReadStream<T> implements ReadStream<T> {

    private static final Logger log = LoggerFactory.getLogger(RecordReadStream.class.getName());

    private Handler<Throwable> exceptionHandler;
    private Handler<T> dataHandler;
    private Handler<Void> endHandler;

    private AtomicBoolean paused = new AtomicBoolean(false);
    private AtomicBoolean finished = new AtomicBoolean(false);
    private Iterator<T> inputIterator;

    public RecordReadStream(Stream<T> stream) {
        this.inputIterator = stream.iterator();
    }

    public RecordReadStream(Iterator<T> iterator) {
        this.inputIterator = iterator;
    }

    public RecordReadStream(Iterable<T> iterable) {
        this.inputIterator = iterable.iterator();
    }

    private void readData() {
        log.debug("readData() called");
        if(!finished.get()) {
            try {
                while (this.inputIterator.hasNext()) {
                    if (paused.get()) {
                        return;
                    }
                    if (this.dataHandler != null) {
                        log.debug("send next data item to dataHandler");
                        this.dataHandler.handle(this.inputIterator.next());
                    }
                }
                if(this.endHandler != null) {
                    this.finished.set(true);
                    this.endHandler.handle(null);
                }
            } catch (Throwable t) {
                log.error(t.getMessage());
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(t);
                }
            }
        }
    }

    @Override
    public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
        log.debug("exceptionHandler() called");
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public ReadStream<T> handler(Handler<T> handler) {
        log.debug("handler() called");
        this.dataHandler = handler;
        readData();
        return this;
    }

    @Override
    public ReadStream<T> pause() {
        log.debug("pause() called");
        this.paused.set(true);
        return this;
    }

    @Override
    public ReadStream<T> resume() {
        log.debug("resume() called");
        this.paused.set(false);
        readData();
        return this;
    }

    @Override
    public ReadStream<T> endHandler(Handler<Void> handler) {
        log.debug("endHandler() called");
        this.endHandler = handler;
        return this;
    }
}

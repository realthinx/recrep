package de.iothings.recrep.common;

import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.MessageProducer;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Created by johannes on 21.12.16.
 */
//public class RecordFileStream {
//
//
//    public static void streamRecordTimeline(Vertx vertx, String address) throws IOException {
//        MessageProducer<Long> recordLineProducer = vertx.eventBus().sender(address);
//        Stream<String> recordLineStream = Files.lines(Paths.get(Constants.RECORDING_FILE_NAME));
//        Observable.from( recordLineStream::iterator ).
//                map( line -> {
//                    return Long.valueOf(line.split("\\|")[0]);
//                }).
//                scan( (lastTimestamp, nextTimestamp) -> {
//                    return ( nextTimestamp - lastTimestamp );
//                }).
//                flatMap( delay -> {
//                    return Observable.timer(delay, TimeUnit.MILLISECONDS, RxHelper.scheduler(vertx));
//                }).
//                subscribe( tick -> {
//                    recordLineProducer.send(1l);
//                });
//    }
//
//    public static void streamRecordPayloadsWithBackpressure(Vertx vertx, String address) throws IOException {
//        MessageProducer<String> recordPayloadProducer = vertx.eventBus().sender(address);
//        Stream<String> recordLineStream = Files.lines(Paths.get(Constants.RECORDING_FILE_NAME));
//        Iterator<String> recordLineIterator = recordLineStream.iterator();
//        while(recordLineIterator.hasNext()) {
//            if(!recordPayloadProducer.writeQueueFull()) {
//                recordPayloadProducer.send(recordLineIterator.next().split("\\|")[0]);
//            }
//        }
//    }
//
//}

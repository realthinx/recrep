package de.iothings.recrep.common;

import de.iothings.recrep.model.RecrepRecordJobFields;
import de.iothings.recrep.model.RecrepReplayJobFields;
import de.iothings.recrep.pubsub.EventPublisher;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Observable;
import java.util.stream.Stream;

/**
 * Created by johannes on 23.12.16.
 */
public class RecordLogHelper {

    private Vertx vertx;
    private RecrepLogHelper log;
    private Integer rollingLogFileCount = 9;
    private Integer defaultLogFileSizeMb = 10;

    public RecordLogHelper(Vertx vertx) {
        this.vertx = vertx;
        this.log = new RecrepLogHelper(vertx, RecordLogHelper.class.getName());
    }

    public Logger createAndGetRecordLogger(JsonObject recordJob) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();

        String recordJobName = recordJob.getString(RecrepRecordJobFields.NAME);
        String recordFilePath = recordJob.getString(RecrepRecordJobFields.FILE_PATH);

        Layout layout = PatternLayout.newBuilder()
                .withPattern("%d{UNIX_MILLIS}|%m%n")
                .build();

        Integer fileSize = recordJob.getInteger(RecrepRecordJobFields.MAX_SIZE_MB);
        if(fileSize == null) {
            fileSize = defaultLogFileSizeMb;
        }
        String sizeString = (fileSize * 1024) / (rollingLogFileCount + 1) + "KB";
        log.debug("Sizebased Policy with size: " + sizeString);
        TriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(sizeString);
        DefaultRolloverStrategy defaultRolloverStrategy = DefaultRolloverStrategy.createStrategy(rollingLogFileCount+"", null,null,null,null,false, config);

        Appender appender = RollingFileAppender.newBuilder()
                .withName(recordJobName)
                .withAppend(true)
                .withFileName((recordFilePath != null ? recordFilePath + File.separator : "") + recordJobName + ".log")
                .withConfiguration(config)
                .withFilePattern((recordFilePath != null ? recordFilePath + File.separator : "") + recordJobName + "-%i.log")
                .withLayout(layout)
                .withPolicy(sizeBasedTriggeringPolicy)
                .withStrategy(defaultRolloverStrategy)
                .build();

        appender.start();

        config.addAppender(appender);
        AppenderRef ref = AppenderRef.createAppenderRef(recordJobName, null, null);
        AppenderRef[] refs = new AppenderRef[] {ref};

        LoggerConfig loggerConfig = LoggerConfig.createLogger(false, Level.INFO, recordJobName, "true", refs, null, config, null );

        loggerConfig.addAppender(appender, null, null);
        config.addLogger(recordJobName, loggerConfig);
        ctx.updateLoggers();

        log.debug("Created log4j2 logger for record job and source: " + recordJobName);
        return LoggerFactory.getLogger(recordJobName);
    }

    public void removeRecordLogger(String recordJobName) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        config.removeLogger(recordJobName);
        ctx.updateLoggers();
        log.debug("Removed log4j2 logger for record job and source: " + recordJobName);
    }

    public Stream<String> getRecordLogFileStream(JsonObject replayJob) {

        try {

            ArrayList<Path> pathArrayList = getRecordLogFiles(replayJob);

           return pathArrayList.stream()
                   //.sorted()
                   .map(path -> {
                try {
                    return Files.lines(path);
                } catch (IOException e) {
                    log.error("Failed to read record log file: " + path + ": " + e.getMessage());
                }
                return null;
            }).flatMap(stringStream -> stringStream);

        } catch (Exception x) {
            log.error("Failed to read record log file: " + replayJob.getString(RecrepReplayJobFields.RECORDJOBNAME) + ".log :" + x.getMessage());
            return null;
        }
    }

    public Long getRecordLogFileSize(JsonObject job) {
        ArrayList<Path> pathArrayList = getRecordLogFiles(job);
        return pathArrayList.stream()
                .mapToLong(path -> path.toFile().length())
                .sum();
    }

    private ArrayList<Path> getRecordLogFiles(JsonObject job) {
        String jobName = "";
        try {
            Path dir = FileSystems.getDefault().getPath(job.getString(RecrepReplayJobFields.FILE_PATH));
            jobName = (job.getString(RecrepReplayJobFields.RECORDJOBNAME)!=null?job.getString(RecrepReplayJobFields.RECORDJOBNAME):job.getString(RecrepRecordJobFields.NAME));
            DirectoryStream<Path> stream = Files.newDirectoryStream( dir, jobName+"*.{log}" );

            ArrayList<Path> pathArrayList = new ArrayList<>();
            stream.forEach(pathArrayList::add);
            stream.close();
            return pathArrayList;

        } catch (Exception x) {
            log.error("Failed to read record log file: " + jobName + ".log :" + x.getMessage());
            return new ArrayList<Path>();
        }
    }

}

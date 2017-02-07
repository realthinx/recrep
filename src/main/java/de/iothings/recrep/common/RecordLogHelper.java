package de.iothings.recrep.common;

import de.iothings.recrep.model.RecrepRecordJobFields;
import de.iothings.recrep.model.RecrepReplayJobFields;
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

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Observable;
import java.util.stream.Stream;

/**
 * Created by johannes on 23.12.16.
 */
public class RecordLogHelper {

    private static final Logger log = LoggerFactory.getLogger(RecordLogHelper.class.getName());

    public static Logger createAndGetRecordLogger(JsonObject recordJob) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();

        String recordJobName = recordJob.getString(RecrepRecordJobFields.NAME);
        String recordFilePath = recordJob.getString(RecrepRecordJobFields.FILE_PATH);

        Layout layout = PatternLayout.newBuilder()
                .withPattern("%d{UNIX_MILLIS}|%m%n")
                .build();

        String fileSize = recordJob.getString(RecrepRecordJobFields.MAX_SIZE_MB);
        if(fileSize == null || fileSize.length() == 0) {
            fileSize = "10 MB";
        }
        TriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(fileSize);
        DefaultRolloverStrategy defaultRolloverStrategy = DefaultRolloverStrategy.createStrategy("10", null,null,null,null,false,config);

        Appender appender = RollingFileAppender.newBuilder()
                .withName(recordJobName)
                .withAppend(true)
                .withFileName((recordFilePath != null ? recordFilePath + "/" : "") + recordJobName + ".log")
                .withConfiguration(config)
                .withFilePattern((recordFilePath != null ? recordFilePath + "/" : "") + recordJobName + "-%i.log")
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

    public static void removeRecordLogger(String recordJobName) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        config.removeLogger(recordJobName);
        ctx.updateLoggers();
        log.debug("Removed log4j2 logger for record job and source: " + recordJobName);
    }

    public static Stream<String> getRecordLogFileStream(JsonObject replayJob) {

        try {

            Path dir = FileSystems.getDefault().getPath(replayJob.getString(RecrepReplayJobFields.FILE_PATH));
            DirectoryStream<Path> stream = Files.newDirectoryStream( dir, replayJob.getString(RecrepReplayJobFields.RECORDJOBNAME)+"*.{log}" );

            ArrayList<Path> pathArrayList = new ArrayList<>();
            stream.forEach(pathArrayList::add);
            stream.close();

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

}

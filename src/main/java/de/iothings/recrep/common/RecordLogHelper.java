package de.iothings.recrep.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by johannes on 23.12.16.
 */
public class RecordLogHelper {

    private static final Logger log = LoggerFactory.getLogger(RecordLogHelper.class.getName());

    public static Logger createAndGetRecordLogger(String recordJobName) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();

        Layout layout = PatternLayout.newBuilder()
                .withPattern("%d{UNIX_MILLIS}|%m%n")
                .build();

        TriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy("1024 KB");

        Appender appender = RollingFileAppender.newBuilder()
                .withName(recordJobName)
                .withAppend(true)
                .withFileName(recordJobName + ".log")
                .withConfiguration(config)
                .withFilePattern(recordJobName + ".log-%i")
                .withLayout(layout)
                .withPolicy(sizeBasedTriggeringPolicy)
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

}

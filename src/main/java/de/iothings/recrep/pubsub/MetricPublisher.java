package de.iothings.recrep.pubsub;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import de.iothings.recrep.common.RecordLogHelper;
import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.util.HashMap;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by johannes on 24.12.16.
 */
public class MetricPublisher {

    private RecrepLogHelper log;
    private Vertx vertx;
    private String METRIC_ADRESS_PREFIX = RecrepSignalType.METRICS + "-";
    private String eventBusAddress;
    private long timestampStart;
    private HashMap<String, Counter> messageCounters;
    private HashMap<String, Meter> messageMeters;
    private HashMap<String, Counter> sizeCounters;
    private HashMap<String, Meter> sizeMeters;
    private Counter diskSpaceUsedCounter;
    private Counter diskSpaceAvailableCounter;
    private MetricRegistry metrics;
    private boolean endlessJob;
    private String jobName;

    public MetricPublisher(Vertx vertx, JsonObject job) {
        this.jobName = job.getString(RecrepReplayJobFields.NAME);
        this.endlessJob = (job.getLong(RecrepRecordJobFields.TIMESTAMP_END) == null);
        this.vertx = vertx;
        this.metrics = new MetricRegistry();
        this.eventBusAddress = METRIC_ADRESS_PREFIX + jobName;
        this.log = new RecrepLogHelper(vertx, MetricPublisher.class.getName());
        this.messageCounters = new HashMap<>();
        this.messageMeters = new HashMap<>();
        this.sizeCounters = new HashMap<>();
        this.sizeMeters = new HashMap<>();
        this.diskSpaceUsedCounter = metrics.counter(name(jobName + "_diskSpaceUsedCounter"));
        this.diskSpaceAvailableCounter = metrics.counter(name(jobName + "_diskSpaceAvailableCounter"));
        this.timestampStart = System.currentTimeMillis();
    }

    public void publishMessageMetrics(String endpointIdentifier, long bytes) {
        if(!endlessJob) {
            countMessage(endpointIdentifier);
            countSize(endpointIdentifier, bytes);
        }
        meterSize(endpointIdentifier, bytes);
        meterMessage(endpointIdentifier);
        publishMetricsUpdate(endpointIdentifier);
    }

    public void countResourceMetrics(long bytes, String path) {
        countDiskSpaceUsed(bytes);
        countDiskSpaceAvailable(path);
    }

    private void publishMetricsUpdate(String endpointIdentifier) {
        vertx.eventBus().publish(eventBusAddress, new JsonObject().put("endpointIdentifier", endpointIdentifier)
                .put("metrics", getMetric(endpointIdentifier)));
        log.debug("Published to: " + eventBusAddress + " - Identifier: " + endpointIdentifier + " - " + getMetric(endpointIdentifier).toString());
    }

    private JsonObject getMetric(String endpointIdentifier) {

        JsonObject metric = new JsonObject();

        if(!endlessJob) {
            metric
                .put(RecrepJobMetricFields.MESSAGE_COUNT, messageCounters.get(endpointIdentifier).getCount())
                .put(RecrepJobMetricFields.MESSAGE_SIZE_BYTES, sizeCounters.get(endpointIdentifier).getCount());
        }
        metric
           .put(RecrepJobMetricFields.MESSAGE_AVERAGE_RATE_SECOND, messageMeters.get(endpointIdentifier).getMeanRate())
           .put(RecrepJobMetricFields.MESSAGE_AVERAGE_SIZE_BYTES, sizeMeters.get(endpointIdentifier).getMeanRate())
           .put(RecrepJobMetricFields.DISK_SIZE_USED, diskSpaceUsedCounter.getCount())
           .put(RecrepJobMetricFields.DISK_SIZE_AVAILABLE, diskSpaceAvailableCounter.getCount());

        return metric;
    }

    private void countMessage(String endpointIdentifier) {
        if (!messageCounters.containsKey(endpointIdentifier)) {
            Counter counter = metrics.counter(name(endpointIdentifier + "_messageCounter"));
            messageCounters.put(endpointIdentifier, counter);
            counter.inc();
        } else {
            messageCounters.get(endpointIdentifier).inc();
        }
    }

    private void meterMessage(String endpointIdentifier) {
        if (!messageMeters.containsKey(endpointIdentifier)) {
            Meter meter = metrics.meter(name(endpointIdentifier+ "_messageMeter"));
            messageMeters.put(endpointIdentifier, meter);
            meter.mark();
        } else {
            messageMeters.get(endpointIdentifier).mark();
        }
    }

    private void countSize(String endpointIdentifier, long bytes) {
        if (!sizeCounters.containsKey(endpointIdentifier)) {
            Counter counter = metrics.counter(name(endpointIdentifier + "_sizeCounter"));
            sizeCounters.put(endpointIdentifier, counter);
            counter.inc(bytes);
        } else {
            sizeCounters.get(endpointIdentifier).inc(bytes);
        }
    }

    private void meterSize(String endpointIdentifier, long bytes) {
        if (!sizeMeters.containsKey(endpointIdentifier)) {
            Meter meter = metrics.meter(name(endpointIdentifier+ "_sizeMeter"));
            sizeMeters.put(endpointIdentifier, meter);
            meter.mark(bytes);
        } else {
            sizeMeters.get(endpointIdentifier).mark(bytes);
        }
    }

    private void countDiskSpaceUsed(long bytes) {
        if(diskSpaceUsedCounter.getCount() < bytes) {
            diskSpaceUsedCounter.inc(bytes - diskSpaceUsedCounter.getCount());
        } else {
            diskSpaceUsedCounter.dec(diskSpaceUsedCounter.getCount() - bytes);
        }
    }

    private void countDiskSpaceAvailable(String path) {
        try {
            File file = new File(path);
            long freeSpace = file.getFreeSpace();
            if(diskSpaceAvailableCounter.getCount() < freeSpace) {
                diskSpaceAvailableCounter.inc(freeSpace - diskSpaceAvailableCounter.getCount());
            } else {
                diskSpaceAvailableCounter.dec(diskSpaceAvailableCounter.getCount() - freeSpace);
            }
        } catch (Exception x) {
            // silent
        }

    }

}

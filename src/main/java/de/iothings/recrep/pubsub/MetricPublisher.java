package de.iothings.recrep.pubsub;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.EventBusAddress;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by johannes on 24.12.16.
 */
public class MetricPublisher {

    private RecrepLogHelper log;
    private Vertx vertx;
    private String METRIC_ADRESS_PREFIX = "METRICS-";
    private String eventBusAddress;
    private HashMap<String, AtomicInteger> counters;

    public MetricPublisher(Vertx vertx, String jobName) {
        this.vertx = vertx;
        this.eventBusAddress = METRIC_ADRESS_PREFIX + jobName;
        this.log = new RecrepLogHelper(vertx, MetricPublisher.class.getName());
        this.counters = new HashMap<>();
    }

    public void publishMessageCount(String endpointIdentifier) {
        vertx.eventBus().publish(eventBusAddress, new JsonObject().put("endpointIdentifier", endpointIdentifier).put("count", getCount(endpointIdentifier)));
        log.debug("Published to: " + eventBusAddress + " - Event: " + endpointIdentifier);
    }

    private int getCount(String endpointIdentifier) {
        if (!counters.containsKey(endpointIdentifier)) {
            AtomicInteger counter = new AtomicInteger();
            counters.put(endpointIdentifier, counter);
            return counter.incrementAndGet();
        } else {
            return counters.get(endpointIdentifier).incrementAndGet();
        }
    }

}

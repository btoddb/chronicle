package com.btoddb.chronicle;

import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;


/**
 *
 */
public class PlunkerMetrics extends ChronicleMetrics {
    private static ThreadLocal<PlunkerMetricsContext> metrics = new ThreadLocal<PlunkerMetricsContext>() {
        @Override
        protected PlunkerMetricsContext initialValue() {
            return new PlunkerMetricsContext();
        }
    };

    public PlunkerMetrics() {
        super("plunkers");
    }

    public void initialize(String componentId) {
        updateMetrics(componentId, null);
    }

    private void updateMetrics(String componentId, PlunkerMetricsContext context) {
        if (null != context) {
            registry.timer(name(componentId, "event-rate")).update(context.getPerEventDuration(), TimeUnit.MICROSECONDS);
            registry.histogram(name(componentId, "batch-size")).update(context.getBatchSize());
        }
        else {
            registry.timer(name(componentId, "event-rate"));
            registry.histogram(name(componentId, "batch-size"));
        }
    }

    public void markBatchStart(String componentId) {
        metrics.get().startBatch();
    }

    public void markBatchEnd(String componentId) {
        metrics.get().endBatch();
        updateMetrics(componentId, metrics.get());
        metrics.remove();
    }

    public void setBatchSize(int batchSize) {
        metrics.get().setBatchSize(batchSize);
    }

}

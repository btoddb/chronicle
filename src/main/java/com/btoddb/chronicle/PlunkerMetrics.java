package com.btoddb.chronicle;

/*
 * #%L
 * chronicle
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

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

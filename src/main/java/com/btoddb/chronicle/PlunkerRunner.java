package com.btoddb.chronicle;

/*
 * #%L
 * fast-persistent-queue
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

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqBatchCallback;
import com.btoddb.fastpersitentqueue.FpqBatchReader;
import com.btoddb.fastpersitentqueue.FpqEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Runs the plunker - handling the mundane part of popping from FPQ, etc that all
 * plunkers must do.
 */
public class PlunkerRunner implements ChronicleComponent, FpqBatchCallback {
    private static final Logger logger = LoggerFactory.getLogger(PlunkerRunner.class);

    private Config config;
    private FpqBatchReader batchReader;
    protected PlunkerMetrics metrics;

    private String id;
    private Plunker plunker;
    private Fpq fpq;

    @Override
    public void init(Config config) throws Exception {
        this.config = config;
        this.metrics = config.getPlunkerMetrics();

        initializeComponent(plunker, id);

        if (null == fpq.getQueueName()) {
            fpq.setQueueName(id);
        }
        fpq.init();

        batchReader = new FpqBatchReader();
        batchReader.setFpq(fpq);
        batchReader.setCallback(this);
        batchReader.init();
        // see this.available()
        batchReader.start();
    }

    private void initializeComponent(ChronicleComponent component, String id) throws Exception {
        if (null == component.getId()) {
            component.setId(id);
        }
        component.init(config);
    }

    /**
     * Send events to the FPQ associated with the {@link Plunker}.  Handles
     * all TX management - will rollback if plunker throws any exceptions and then rethrow exception.
     *
     * @param events Collection of {@link Event}
     */
    public void run(Collection<Event> events) {
        Fpq fpq = getFpq();

        fpq.beginTransaction();
        try {
            for (Event event : events) {
                fpq.push(config.getEventSerializer().serialize(event));
            }
            fpq.commit();
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, String.format("exception while routing events to plunker, %s", plunker), e);
        }
        finally {
            if (fpq.isTransactionActive()) {
                fpq.rollback();
            }
        }
    }

    @Override
    public void available(Collection<FpqEntry> entries) throws Exception {
        List<Event> eventList = new ArrayList<>(entries.size());
        for (FpqEntry entry : entries) {
            Event event = config.getEventSerializer().deserialize(entry.getData());
            eventList.add(event);
        }

        plunker.handle(eventList);
    }

    public void shutdown() {
        if (null != batchReader) {
            try {
                batchReader.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ batch reader", e);
            }
        }

        if (null != fpq) {
            try {
                fpq.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ", e);
            }
        }

        if (null != plunker) {
            try {
                plunker.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down plunker, {}", plunker.getId(), e);
            }
        }
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public Config getConfig() {
        return null;
    }

    @Override
    public void setConfig(Config config) {

    }

    public Plunker getPlunker() {
        return plunker;
    }

    public void setPlunker(Plunker plunker) {
        this.plunker = plunker;
    }

    public Fpq getFpq() {
        return fpq;
    }

    public void setFpq(Fpq fpq) {
        this.fpq = fpq;
    }
}

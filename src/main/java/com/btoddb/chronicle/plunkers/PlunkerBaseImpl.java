package com.btoddb.chronicle.plunkers;

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

import com.btoddb.chronicle.ChronicleComponentBaseImpl;
import com.btoddb.chronicle.Config;
import com.btoddb.chronicle.Event;
import com.btoddb.chronicle.Plunker;
import com.btoddb.chronicle.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 *
 */
public abstract class PlunkerBaseImpl extends ChronicleComponentBaseImpl implements Plunker {
    private static final Logger logger = LoggerFactory.getLogger(PlunkerBaseImpl.class);


    /**
     * Derived classes implement this method to process the Collection of events.  Throwing any exception
     * will cause a rollback, otherwise the transaction is committed.
     *
     * @param events Collection of {@link com.btoddb.chronicle.Event}s
     * @throws Exception
     */
    protected abstract void handleInternal(Collection<Event> events) throws Exception;


    @Override
    public void init(Config config) throws Exception {
        super.init(config);
    }

    @Override
    public final void handle(Collection<Event> events) throws Exception {
        try {
            config.getCatcherMetrics().getRegistry().meter(getId()).mark(events.size());
            handleInternal(events);
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, String.format("exception while handling events in plunker, %s", getId()), e);
        }
    }
}
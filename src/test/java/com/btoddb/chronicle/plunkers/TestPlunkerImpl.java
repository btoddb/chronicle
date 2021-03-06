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

import com.btoddb.chronicle.Config;
import com.btoddb.chronicle.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


/**
 */
public class TestPlunkerImpl extends PlunkerBaseImpl {
    private static Logger logger = LoggerFactory.getLogger(TestPlunkerImpl.class);

    private List<Event> eventList = new LinkedList<Event>();

    public TestPlunkerImpl() {
        super(null);
    }

    @Override
    public void init(Config config) throws Exception {
        super.init(config);
    }

    @Override
    protected void handleInternal(Collection<Event> events) throws Exception {
        eventList.addAll(events);
    }

    @Override
    public void shutdown() {
    }

    public List<Event> waitForEvents(int minNumEvents, long maxWaitInMillis) {
        long endTime = System.currentTimeMillis()+maxWaitInMillis;
        while(eventList.size() < minNumEvents && System.currentTimeMillis() <= endTime) {
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                // do nothing
                Thread.interrupted();
            }
        }
        return eventList;
    }
}

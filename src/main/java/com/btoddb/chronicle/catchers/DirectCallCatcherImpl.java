package com.btoddb.chronicle.catchers;

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

import com.btoddb.chronicle.Catcher;
import com.btoddb.chronicle.Config;
import com.btoddb.chronicle.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 * Call this catcher directly from code to inject events into Chronicle
 */
public class DirectCallCatcherImpl extends CatcherBaseImpl implements Catcher, DirectCallCatcher {
    private static final Logger logger = LoggerFactory.getLogger(DirectCallCatcherImpl.class);


    public DirectCallCatcherImpl() {}

    // this constructor is to get around what seems to be a problem with SnakeYaml wanting a single arg constructor
    // when there are no properties for the object
    public DirectCallCatcherImpl(String dummy) {}

    @Override
    public void init(Config config) throws Exception {
        super.init(config);
    }

    @Override
    public void shutdown() {
        // nothing to do
    }

    @Override
    public void inject(Collection<Event> events) {
        catchEvents(events);
    }
}

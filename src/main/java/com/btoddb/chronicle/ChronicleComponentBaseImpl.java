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


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;


/**
 * All components managed by Chronicle should derive from this abstract class.
 */
public abstract class ChronicleComponentBaseImpl implements ChronicleComponent {
    protected Config config;
    protected String id;

    private String componentType;

    @Override
    public void init(Config config) throws Exception {
        this.config = config;
        determineComponentType();
        config.getGeneralMetrics().getRegistry().register(name(getId(), "type"), new Gauge<String>() {
            @Override
            public String getValue() {
                return componentType;
            }
        });
    }

    void determineComponentType() {
        if (Catcher.class.isAssignableFrom(this.getClass())) {
            componentType = "catcher";
        }
        else if (Router.class.isAssignableFrom(this.getClass())) {
            componentType = "router";
        }
        else if (Plunker.class.isAssignableFrom(this.getClass())) {
            componentType = "plunker";
        }
        else if (Snooper.class.isAssignableFrom(this.getClass())) {
            componentType = "snooper";
        }
        else if (ErrorHandler.class.isAssignableFrom(this.getClass())) {
            componentType = "error-handler";
        }
        else {
            componentType = "unknown";
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
        return config;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }
}

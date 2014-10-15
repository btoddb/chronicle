package com.btoddb.chronicle.plunkers.hdfs;

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

import com.btoddb.chronicle.ChronicleException;
import com.btoddb.chronicle.TokenValueProvider;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


/**
 *
 */
public class HdfsTokenValueProvider implements TokenValueProvider {
//    private static final Logger logger = LoggerFactory.getLogger(HdfsTokenValueProvider.class);

    private static final Set<String> AVAILABLE_TOKENS = new HashSet<>();
    static {
        AVAILABLE_TOKENS.add("timestamp");
    }

    private Cache<String, Object> values = CacheBuilder.newBuilder().build();

    @Override
    public boolean hasValueFor(String token) {
        return AVAILABLE_TOKENS.contains(token);
    }

    @Override
    public String getValue(final String token) {
        return String.valueOf(getFromCache(token));
    }

    Object getFromCache(final String key) {
        try {
            Object obj = values.get(key, new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    switch (key) {
                        case "timestamp":
                            return System.currentTimeMillis();

                        default:
                            throw new ChronicleException(String.format("Unknown token name, '%s' - cannot supply value from provider, %s", key, HdfsTokenValueProvider.class.getName()));
                    }
                }
            });
            return obj;
        }
        catch (ExecutionException e) {
            throw new ChronicleException("unexpected exception while providing token value", e);
        }
    }
}

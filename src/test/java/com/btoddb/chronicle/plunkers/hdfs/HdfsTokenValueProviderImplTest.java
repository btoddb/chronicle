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

import junit.framework.TestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class HdfsTokenValueProviderImplTest extends TestCase {
    HdfsTokenValueProviderImpl provider = new HdfsTokenValueProviderImpl();

    public void testHasValueFor() throws Exception {

    }

    public void testGetValue() throws Exception {

    }

    public void testGetFromCache() throws Exception {
        long now = (long) provider.getFromCache("now");
        assertThat((long) provider.getFromCache("now"), is(now));
        Thread.sleep(1);
        assertThat((long) provider.getFromCache("now"), is(now));
        Thread.sleep(1);
        assertThat((long) provider.getFromCache("now"), is(now));
        Thread.sleep(1);

    }
}
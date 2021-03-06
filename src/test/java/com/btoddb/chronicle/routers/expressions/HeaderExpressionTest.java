package com.btoddb.chronicle.routers.expressions;

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

import com.btoddb.chronicle.Event;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class HeaderExpressionTest {

    @Test
    public void testHeaderMatcherEqualTrue() {
        HeaderExpression exp = new HeaderExpression("the-header", "=", "val.+");

        Event event = new Event(new byte[]{});
        event.withHeader("foo", "bar")
                .withHeader("the-header", "target-value");
        assertThat(exp.match(event), is(true));
    }

    @Test
    public void testHeaderMatcherEqualFalse() {
        HeaderExpression exp = new HeaderExpression("the-header", "=", "val.+");

        Event event = new Event(new byte[]{})
                .withHeader("foo", "bar")
                .withHeader("the-header", "target-miss");
        assertThat(exp.match(event), is(false));
    }

    @Test
    public void testHeaderMatcherNotEqualTrue() {
        HeaderExpression exp = new HeaderExpression("the-header", "!=", "val.+");

        Event event = new Event(new byte[]{})
                .withHeader("foo", "bar")
                .withHeader("the-header", "target");
        assertThat(exp.match(event), is(true));
    }

    @Test
    public void testHeaderMatcherNotEqualFalse() {
        HeaderExpression exp = new HeaderExpression("the-header", "!=", "val.+");

        Event event = new Event(new byte[]{})
                .withHeader("foo", "bar")
                .withHeader("the-header", "target-value");
        assertThat(exp.match(event), is(false));
    }

}
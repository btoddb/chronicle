package com.btoddb.chronicle.serializers;

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

import java.io.IOException;
import java.io.OutputStream;


/**
 * Interface for FpqEvent serializers.  Serializers are used to transform an {@link com.btoddb.chronicle.Event}
 * object into a byte stream.
 */
public interface EventSerializer {

    /**
     * Preferred method to stream directly to output stream.
     *
     * @param outStream
     * @param event
     * @throws IOException
     */
    void serialize(OutputStream outStream, Event event) throws IOException;

    /**
     * Optional - needed for formats like Avro to convert an {@link com.btoddb.chronicle.Event} to
     * an object that the underlying file writer can handle.
     *
     * @param event
     * @return Object compatible with respective file writer
     */
    Object convert(Event event);
}

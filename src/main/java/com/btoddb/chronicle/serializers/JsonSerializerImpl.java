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

import com.btoddb.chronicle.ChronicleException;
import com.btoddb.chronicle.Config;
import com.btoddb.chronicle.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;


/**
 *
 */
public class JsonSerializerImpl extends EventSerializerBaseImpl {
    private static final ObjectMapper objectMapper = new ObjectMapper();


    // this constructor is to get around what seems to be a problem with SnakeYaml wanting a single arg constructor
    public JsonSerializerImpl() {}
    public JsonSerializerImpl(String dummy) {}

    @Override
    public void serialize(OutputStream outStream, Event event) throws IOException {
        outStream.write(objectMapper.writeValueAsBytes(event));
        outStream.write('\n');
    }

    public byte[] serialize(Event event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        }
        catch (JsonProcessingException e) {
            throw new ChronicleException("exception while serializing Event to byte array", e);
        }
    }

    public void serialize(PrintWriter printWriter, Event event) {
        try {
            objectMapper.writeValue(printWriter, event);
        }
        catch (IOException e) {
            throw new ChronicleException("exception while serializing Event to PrintWriter", e);
        }
    }

    public Event deserialize(BufferedInputStream reqInStream) {
        try {
            return objectMapper.readValue(reqInStream, Event.class);
        }
        catch (IOException e) {
            throw new ChronicleException("exception while deserializing Event", e);
        }
    }

    public Event deserialize(byte[] data) {
        return deserialize(new BufferedInputStream(new ByteArrayInputStream(data)));
    }

    public Event deserialize(String data) {
        return deserialize(data.getBytes());
    }

    public List<Event> deserializeList(BufferedInputStream reqInStream) {
        try {
            return objectMapper.readValue(reqInStream, new TypeReference<List<Event>>() {});
        }
        catch (IOException e) {
            throw new ChronicleException("exception while deserializing Event list", e);
        }
    }
}

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

import com.btoddb.chronicle.Event;
import com.btoddb.chronicle.serializers.EventSerializer;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 *
 */
public class HdfsAvroFileImpl extends HdfsFileBaseImpl {
    private static final Logger logger = LoggerFactory.getLogger(HdfsAvroFileImpl.class);

    private static final String SCHEMA_JSON = "{" +
            "  \"fields\": [" +
            "    {" +
            "      \"name\": \"headers\", " +
            "      \"type\": {" +
            "        \"type\": \"map\", \"avro.java.string\": \"String\", " +
            "        \"values\": {" +
            "          \"type\": \"string\", \"avro.java.string\": \"String\"" +
            "        }" +
            "      }" +
            "    }, " +
            "    {" +
            "      \"name\": \"body\", " +
            "      \"type\": {" +
            "        \"type\": \"string\", \"avro.java.string\": \"String\"" +
            "      }" +
            "    }" +
            "  ], " +
            "  \"name\": \"Chronicle\", " +
            "  \"type\": \"record\"" +
            "}";
    private static final Schema SCHEMA_AVRO = new Schema.Parser().parse(SCHEMA_JSON);

    private DataFileWriter<Object> dataFileWriter;

    private int syncIntervalBytes = 2*1024*1024;
    private AvroCodecFactory codecFactory;


    @Override
    public void init(String permFilename, String openFilename) throws IOException {
        super.init(permFilename, openFilename);

        dataFileWriter = null;
        DatumWriter<Object> writer = new ReflectDatumWriter<>();
        dataFileWriter = new DataFileWriter<Object>(writer);


        dataFileWriter.setSyncInterval(syncIntervalBytes);
        if (null != codecFactory) {
            dataFileWriter.setCodec(codecFactory.getInstance());
        }

        dataFileWriter.create(SCHEMA_AVRO, outputStream);
    }

    @Override
    public void writeInternal(Event event) throws IOException {
        dataFileWriter.append(serializer.convert(event));
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
        super.close();
    }

    @Override
    public void flush() throws IOException {
        // gotta flush DataFileWriter otherwise Avro data will not be written until sync interval is hit
        dataFileWriter.flush();
        super.flush();
    }

    public AvroCodecFactory getCodecFactory() {
        return codecFactory;
    }

    public void setCodecFactory(AvroCodecFactory codecFactory) {
        this.codecFactory = codecFactory;
    }
}

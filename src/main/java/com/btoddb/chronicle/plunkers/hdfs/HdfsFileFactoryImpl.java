package com.btoddb.chronicle.plunkers.hdfs;

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
import com.btoddb.chronicle.serializers.EventSerializer;


/**
 *
 */
public class HdfsFileFactoryImpl implements HdfsFileFactory {
    private Config config;
    private Class<EventSerializer> serializerType;
    private Class<HdfsFile> hdfsFileType;

    public HdfsFileFactoryImpl(Config config, String hdfsFileType, String serializerType) throws ClassNotFoundException {
        this.config = config;
        this.hdfsFileType = (Class<HdfsFile>) Class.forName(hdfsFileType);
        this.serializerType = (Class<EventSerializer>) Class.forName(serializerType);
    }

    @Override
    public HdfsFile createFile(String permFilename, String openFilename) throws ChronicleException {
        try {
            EventSerializer serializer = serializerType.newInstance();
            serializer.init(config);

            HdfsFile hf = hdfsFileType.newInstance();
            hf.init(permFilename, openFilename, serializer);

            return hf;
        }
        catch (Exception e) {
            throw new ChronicleException("exception while creating HdfsFile object", e);
        }
    }
}

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

import com.btoddb.chronicle.serializers.AvroSerializerImpl;

import java.io.IOException;


/**
 *
 */
public class HdfsAvroFileFactoryImpl implements HdfsFileFactory {
    private AvroCodecFactory codecFactory;
    private AvroSerializerImpl serializer;

    @Override
    public HdfsFile createFile(String permFilename, String openFilename) throws IOException {
        HdfsAvroFileImpl hdfsFile = new HdfsAvroFileImpl();
        hdfsFile.setCodecFactory(codecFactory);
        hdfsFile.setSerializer(serializer);
        hdfsFile.init(permFilename, openFilename);
        return hdfsFile;
    }

    public AvroCodecFactory getCodecFactory() {
        return codecFactory;
    }

    public void setCodecFactory(AvroCodecFactory codecFactory) {
        this.codecFactory = codecFactory;
    }

    public AvroSerializerImpl getSerializer() {
        return serializer;
    }

    public void setSerializer(AvroSerializerImpl serializer) {
        this.serializer = serializer;
    }
}

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public abstract class HdfsFileBaseImpl implements HdfsFile {
    protected FSDataOutputStream outputStream;
    protected EventSerializer serializer;

    private String openFilename;
    private String permFilename;
    private FileSystem fileSystem;
    private ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();


    protected abstract void writeInternal(Event event) throws IOException;

    @Override
    public void init(String permFilename, String openFilename) throws IOException {
        this.permFilename = permFilename;
        this.openFilename = openFilename;

        Configuration conf = new Configuration();
        Path path = new Path(this.openFilename);
        fileSystem = path.getFileSystem(conf);
        outputStream = fileSystem.create(path);
    }

    @Override
    public void write(Event event) throws IOException {
        closeLock.readLock().lock();
        try {
            writeInternal(event);
        }
        finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        outputStream.hflush();
        // TODO:BTB - not sure if i need to flush+hsync
        outputStream.hsync();
    }

    @Override
    public void close() throws IOException {
        closeLock.writeLock().lock();
        try {
            if (null != outputStream) {
                outputStream.close();
                outputStream = null;
            }
        }
        finally {
            closeLock.writeLock().unlock();
        }

        renameToPerm();
    }

    void renameToPerm() throws IOException {
        fileSystem.rename(new Path(openFilename), new Path(permFilename));
    }

    @Override
    public String getOpenFilename() {
        return openFilename;
    }

    @Override
    public String getPermFilename() {
        return permFilename;
    }

    public EventSerializer getSerializer() {
        return serializer;
    }

    public void setSerializer(EventSerializer serializer) {
        this.serializer = serializer;
    }
}

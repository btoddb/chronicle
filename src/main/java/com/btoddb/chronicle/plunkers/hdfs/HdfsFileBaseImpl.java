package com.btoddb.chronicle.plunkers.hdfs;

import com.btoddb.chronicle.Event;
import com.btoddb.chronicle.serializers.EventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by burrb009 on 10/16/14.
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
    public void init(String permFilename, String openFilename, EventSerializer serializer) throws IOException {
        this.permFilename = permFilename;
        this.openFilename = openFilename;
        this.serializer = serializer;

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
}

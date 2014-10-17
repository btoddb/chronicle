package com.btoddb.chronicle.plunkers.hdfs;

import com.btoddb.chronicle.Event;
import com.btoddb.chronicle.serializers.EventSerializer;

import java.io.IOException;


/**
 *
 */
public interface HdfsFile {

    void init(String permFilename, String openFilename, EventSerializer serializer) throws IOException;

    void write(Event event) throws IOException;

    void flush() throws IOException;

    void close() throws IOException;

    String getPermFilename();

    String getOpenFilename();
}

package com.btoddb.chronicle.plunkers.hdfs;

import com.btoddb.chronicle.Event;

import java.io.IOException;


/**
 * Writes to a text file using the specified {@link com.btoddb.chronicle.serializers.EventSerializer}.
 */
public class HdfsTextFileImpl extends HdfsFileBaseImpl {

    @Override
    public void writeInternal(Event event) throws IOException {
        serializer.serialize(outputStream, event);
    }

}

package com.btoddb.chronicle.serializers;

import com.btoddb.chronicle.Event;
import com.btoddb.chronicle.plunkers.hdfs.StorableAvroEvent;


/**
 *
 */
public class AvroSerializerImpl extends EventSerializerBaseImpl {

    @Override
    public Object convert(Event event) {
        return new StorableAvroEvent(event);
    }

}

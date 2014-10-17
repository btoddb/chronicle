package com.btoddb.chronicle.serializers;

import com.btoddb.chronicle.ChronicleException;
import com.btoddb.chronicle.Config;
import com.btoddb.chronicle.Event;

import java.io.IOException;
import java.io.OutputStream;


/**
 *
 */
public abstract class EventSerializerBaseImpl implements EventSerializer {
    protected Config config;

    public void init(Config config) {
        this.config = config;
    }

    /**
     * Throws {@link com.btoddb.chronicle.ChronicleException} claiming unsupported.
     *
     * @param outputStream
     * @param event
     * @return nothing
     */
    @Override
    public void serialize(OutputStream outputStream, Event event) throws IOException {
        throw new ChronicleException("Unsupported operation for this serializer, " + getClass().getName());
    }

    /**
     * Throws {@link com.btoddb.chronicle.ChronicleException} claiming unsupported.
     *
     * @param event
     * @return nothing
     */
    @Override
    public Object convert(Event event) {
        throw new ChronicleException("Unsupported operation for this serializer, " + getClass().getName());
    }
}

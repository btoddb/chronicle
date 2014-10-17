package com.btoddb.chronicle.serializers;

import com.btoddb.chronicle.Event;

import java.util.Map;


/**
 *
 */
public class AvroSerializerImpl extends EventSerializerBaseImpl {

    @Override
    public Object convert(Event event) {
        return new AvroEvent(event);
    }

    // --------

    class AvroEvent {
        private Map<String, String> headers;
        private String body;

        public AvroEvent(Event delegate) {
            this.headers = delegate.getHeaders();
            this.body = delegate.getBodyAsString();
        }
        public Map<String, String> getHeaders() {
            return headers;
        }
        public String getBody() {
            return body;
        }
    }
}

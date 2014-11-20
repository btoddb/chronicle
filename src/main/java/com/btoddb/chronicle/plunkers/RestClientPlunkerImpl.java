package com.btoddb.chronicle.plunkers;

import com.btoddb.chronicle.Config;
import com.btoddb.chronicle.Event;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;


/**
 *
 */
public class RestClientPlunkerImpl extends PlunkerBaseImpl {
    private String url;
    private Client client;

    public RestClientPlunkerImpl() {
        super(null);
    }

    @Override
    public void init(Config config) throws Exception {
        super.init(config);
        client = ClientBuilder.newClient();
    }

    @Override
    protected void handleInternal(Collection<Event> events) throws Exception {
        long start = System.currentTimeMillis();

        Response resp = client.target(url)
                .request()
                .post(Entity.entity(events, MediaType.APPLICATION_JSON_TYPE));
        resp.close();
    }

    @Override
    public void shutdown() {
        if (null != client) {
            client.close();
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}

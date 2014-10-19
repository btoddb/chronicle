package com.btoddb.chronicle.apps;

import com.btoddb.chronicle.Event;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 * Created by burrb009 on 10/18/14.
 */
public class RestClient {



    public void go() {
        Event event = new Event("some-data-for-body")
                .withHeader("customer", "btoddb")
                .withHeader("foo", "bar");

        Client client = ClientBuilder.newClient();
        for (int i=0;i < 1000000;i++) {
            Response resp = client.target("http://localhost:8083/v1")
                    .request()
                    .post(Entity.entity(event, MediaType.APPLICATION_JSON_TYPE));
            resp.close();
        }
    }


    public static void main(String[] args) {
        new RestClient().go();
    }

}

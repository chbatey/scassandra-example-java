package com.batey.examples.scassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ExampleDao {

    private int port;
    private Cluster cluster;
    private Session session;

    public ExampleDao(int port) {
        this.port = port;
    }

    public void connect() {
        // do something
        cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build();
        session = cluster.connect("people");

    }

    public List<String> retrieveNames() {
        ResultSet result = session.execute("select * from people");
        return result.all().stream().map(row -> row.getString("name")).collect(Collectors.toList());
    }
}

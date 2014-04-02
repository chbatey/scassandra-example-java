package com.batey.examples.scassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

import java.util.List;
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
        ResultSet result = null;
        try {
            result = session.execute("select * from people");
        } catch (ReadTimeoutException e) {
            throw new ExampleDaoException();
        }
        return result.all().stream().map(row -> row.getString("name")).collect(Collectors.toList());
    }
}

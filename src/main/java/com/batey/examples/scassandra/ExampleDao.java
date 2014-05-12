package com.batey.examples.scassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

import java.util.ArrayList;
import java.util.List;

public class ExampleDao {

    private int port;
    private Cluster cluster;
    private Session session;

    public ExampleDao(int port) {
        this.port = port;
    }

    public void connect() {
        // do something
        cluster = Cluster.builder().addContactPoint("localhost").withPort(port).build();
        session = cluster.connect("people");

    }

    public void disconnect() {
        cluster.close();
    }

    public List<String> retrieveNames() {
        ResultSet result;
        try {
            Statement statement = new SimpleStatement("select * from people");
            statement.setConsistencyLevel(ConsistencyLevel.TWO);
            result = session.execute(statement);
        } catch (ReadTimeoutException e) {
            throw new ExampleDaoException();
        }

        List<String> names = new ArrayList<>();
        for (Row row : result) {
            names.add(row.getString("name"));
        }
        return names;
    }
}

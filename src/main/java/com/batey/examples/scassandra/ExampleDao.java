package com.batey.examples.scassandra;

import com.datastax.driver.core.*;
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
            statement.setConsistencyLevel(ConsistencyLevel.ONE);
            result = session.execute(statement);
        } catch (ReadTimeoutException e) {
            throw new ExampleDaoException();
        }
        return result.all().stream().map(row -> row.getString("name")).collect(Collectors.toList());
    }
}

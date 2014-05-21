package com.batey.examples.scassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

import java.util.ArrayList;
import java.util.List;

public class PersonDao {

    private int port;
    private Cluster cluster;
    private Session session;

    public PersonDao(int port) {
        this.port = port;
    }

    public void connect() {
        cluster = Cluster.builder().addContactPoint("localhost").withPort(port).build();
        session = cluster.connect("people");
    }

    public void disconnect() {
        cluster.close();
    }

    public List<String> retrieveNames() {
        ResultSet result;
        try {
            Statement statement = new SimpleStatement("select * from person");
            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            result = session.execute(statement);
        } catch (ReadTimeoutException e) {
            throw new UnableToRetrievePeopleException();
        }

        List<String> names = new ArrayList<>();
        for (Row row : result) {
            names.add(row.getString("first_name"));
        }
        return names;
    }

}

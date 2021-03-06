package com.batey.examples.scassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class PersonDaoCassandra implements PersonDao {

    private int port;
    private int retries;
    private Cluster cluster;
    private Session session;
    private PreparedStatement storeStatement;
    private PreparedStatement retrieveStatement;

    public PersonDaoCassandra(int port, int retries) {
        this.port = port;
        this.retries = retries;
    }

    @Override
    public void connect() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(500);
        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(port)
                .withRetryPolicy(new LoggingRetryPolicy(new RetryReads()))
                .withSocketOptions(socketOptions)
                .build();
        session = cluster.connect("scassandra");
        storeStatement = session.prepare("insert into person(first_name, last_name, age, interesting_dates) values (?,?,?,?)");
        retrieveStatement = session.prepare("select * from person where first_name = ? and last_name = ?");
    }

    @Override
    public void disconnect() {
        cluster.close();
    }

    @Override
    public List<Person> retrievePeople() {
        ResultSet result;
        try {
            Statement statement = new SimpleStatement("select * from person");
            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            result = session.execute(statement);
        } catch (ReadTimeoutException e) {
            throw new UnableToRetrievePeopleException();
        }

        return result.all().stream().map(
                row -> new Person(row.getString("first_name"), row.getString("last_name"), row.getInt("age"), row.getSet("interesting_dates", Date.class))
        ).collect(Collectors.toList());
    }

    @Override
    public List<Person> retrievePeopleByName(String firstName, String lastName) {
        BoundStatement bind = retrieveStatement.bind(firstName, lastName);
        ResultSet result = session.execute(bind);

        List<Person> people = new ArrayList<>();
        for (Row row : result) {
            people.add(new Person(row.getString("first_name"), row.getString("last_name"), row.getInt("age"), row.getSet("interesting_dates", Date.class)));
        }
        return people;
    }

    @Override
    public void storePerson(Person person) {
        try {
            BoundStatement bind = storeStatement.bind(person.getFirstName(), person.getLastName(), person.getAge(), person.getInterestingDates());
            session.execute(bind);
        } catch (NoHostAvailableException e) {
            throw new UnableToSavePersonException();
        }
    }

    private class RetryReads implements RetryPolicy {
        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            if (nbRetry < retries) {
                return RetryDecision.retry(ConsistencyLevel.ONE);
            } else {
                return RetryDecision.rethrow();
            }
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return DefaultRetryPolicy.INSTANCE.onWriteTimeout(statement, cl, writeType, receivedAcks, receivedAcks, nbRetry);
        }

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return DefaultRetryPolicy.INSTANCE.onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
        }
    }
}

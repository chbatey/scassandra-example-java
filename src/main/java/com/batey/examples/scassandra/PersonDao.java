/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.batey.examples.scassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import java.util.ArrayList;
import java.util.List;

public class PersonDao {

    private int port;
    private int retries;
    private Cluster cluster;
    private Session session;
    private PreparedStatement storeStatement;
    private PreparedStatement retrieveStatement;

    public PersonDao(int port, int retries) {
        this.port = port;
        this.retries = retries;
    }

    public void connect() {
        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(port)
                .withRetryPolicy(new RetryPolicy() {
                    @Override
                    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
                        if (nbRetry < retries) {
                            return RetryDecision.retry(cl);
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
                })
                .build();
        session = cluster.connect("people");
        storeStatement = session.prepare("insert into person(first_name, age) values (?,?)");
        retrieveStatement = session.prepare("select * from person where first_name = ?");
    }

    public void disconnect() {
        cluster.close();
    }

    public List<Person> retrieveNames() {
        ResultSet result;
        try {
            Statement statement = new SimpleStatement("select * from person");
            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            result = session.execute(statement);
        } catch (ReadTimeoutException e) {
            throw new UnableToRetrievePeopleException();
        }

        List<Person> people = new ArrayList<>();
        for (Row row : result) {
            people.add(new Person(row.getString("first_name"), row.getInt("age")));
        }
        return people;
    }

    public List<Person> retrievePeopleByName(String firstName) {
        BoundStatement bind = retrieveStatement.bind(firstName);
        ResultSet result = session.execute(bind);

        List<Person> people = new ArrayList<>();
        for (Row row : result) {
            people.add(new Person(row.getString("first_name"), row.getInt("age")));
        }
        return people;
    }

    public void storePerson(Person person) {
        BoundStatement bind = storeStatement.bind(person.getName(), person.getAge());
        session.execute(bind);
    }

}

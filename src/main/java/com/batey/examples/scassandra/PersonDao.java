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

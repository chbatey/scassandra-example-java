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

import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.http.client.*;
import org.scassandra.junit.ScassandraServerRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class PersonDaoTest {

    @ClassRule
    public static final ScassandraServerRule scassandra = new ScassandraServerRule();
    public static final int CONFIGURED_RETRIES = 3;

    @Rule
    public final ScassandraServerRule resetScassandra = scassandra;

    private static PrimingClient primingClient = scassandra.primingClient();
    private static ActivityClient activityClient = scassandra.activityClient();

    private PersonDao underTest;

    @Before
    public void setup() {
        underTest = new PersonDao(8042, CONFIGURED_RETRIES);
    }

    @After
    public void after() {
        underTest.disconnect();
    }

    @Test
    public void testRetrievingOfNames() throws Exception{
        // given
        Map<String, ? extends  Object> row = ImmutableMap.of(
                "first_name", "Chris",
                "last_name", "Batey",
                "age", 29);
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "age", ColumnTypes.Int
        );
        PrimingRequest singleRowPrime = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withColumnTypes(columnTypes)
                .withRows(row)
                .build();
        primingClient.primeQuery(singleRowPrime);
        underTest.connect();

        //when
        List<Person> names = underTest.retrieveNames();

        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0).getName());
    }


    @Test(expected = UnableToRetrievePeopleException.class)
    public void testHandlingOfReadRequestTimeout() throws Exception {
        // given
        PrimingRequest primeReadRequestTimeout = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        primingClient.primeQuery(primeReadRequestTimeout);

        //when
        underTest.connect();
        underTest.retrieveNames();

        //then
    }

    @Test
    public void shouldConnectToCassandraWhenConnectCalled() {
        //given
        activityClient.clearConnections();
        //when
        underTest.connect();
        //then
        assertTrue("Expected at least one connection to Cassandra on connect",
                activityClient.retrieveConnections().size() > 0);
    }


    @Test
    public void testCorrectQueryIssuedOnConnect() {
        //given
        Query expectedQuery = Query.builder().withQuery("use people").withConsistency("ONE").build();
        //when
        underTest.connect();
        //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue(queries.contains(expectedQuery));
    }

    @Test
    public void testQueryIssuedWithCorrectConsistency() {
        //given
        underTest.connect();
        Query expectedQuery = Query.builder().withQuery("select * from person")
                .withConsistency("QUORUM").build();
        //when
        underTest.retrieveNames();
        //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue("Expected query with consistency QUORUM, found following queries: " + queries,
                queries.contains(expectedQuery));
    }

    @Test
    public void testStorePerson() {
        // given
        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("insert into person(first_name, age) values (?,?)")
                .withVariableTypes(ColumnTypes.Varchar, ColumnTypes.Int)
                .build();
        primingClient.primePreparedStatement(preparedStatementPrime);
        underTest.connect();
        //when
        underTest.storePerson(new Person("Christopher", 29));
        //then
        List<PreparedStatementExecution> executions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, executions.size());
        assertEquals("insert into person(first_name, age) values (?,?)", executions.get(0).getPreparedStatementText());
        assertEquals("ONE", executions.get(0).getConsistency(), "ONE");
        assertEquals(Arrays.asList("Christopher", 29.0), executions.get(0).getVariables());
    }

    @Test
    public void testRetrievePeopleViaPreparedStatement() {
        // given
        Map<String, ? extends  Object> row = ImmutableMap.of(
                "first_name", "Chris",
                "last_name", "Batey",
                "age", 29);
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "age", ColumnTypes.Int
        );
        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from person where first_name = ?")
                .withVariableTypes(ColumnTypes.Varchar, ColumnTypes.Int)
                .withColumnTypes(columnTypes)
                .withRows(row)
                .build();
        primingClient.primePreparedStatement(preparedStatementPrime);
        underTest.connect();
        //when
        List<Person> names = underTest.retrievePeopleByName("Christopher");
        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0).getName());
    }

    @Test
    public void testRetriesConfiguredNumberOfTimes() throws Exception {
        PrimingRequest readtimeoutPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from person")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        primingClient.primeQuery(readtimeoutPrime);
        underTest.connect();
        activityClient.clearAllRecordedActivity();

        try {
            underTest.retrieveNames();
        } catch (UnableToRetrievePeopleException e) {
        }

        assertEquals(CONFIGURED_RETRIES + 1, activityClient.retrieveQueries().size());
    }

    @Ignore("Can't do this until scassandra sends back the consistency that is sent in")
    @Test
    public void testLowersConsistency() throws Exception {
        PrimingRequest readtimeoutPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from person")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        primingClient.primeQuery(readtimeoutPrime);
        underTest.connect();
        activityClient.clearAllRecordedActivity();

        try {
            underTest.retrieveNames();
        } catch (UnableToRetrievePeopleException e) {
        }

        List<Query> queries = activityClient.retrieveQueries();
        assertEquals(Query.builder()
                .withQuery("select * from person")
                .withConsistency("QUORUM").build(), queries.get(0));
        assertEquals(Query.builder()
                .withQuery("select * from person")
                .withConsistency("THREE").build(), queries.get(1));
    }
}

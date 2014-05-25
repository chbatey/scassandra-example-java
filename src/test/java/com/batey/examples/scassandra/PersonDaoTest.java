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
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class PersonDaoTest {

    public static final int DEFAULT_SCASSANDRA_BINARY_PORT = 8042;
    private PersonDao underTest;

    private static PrimingClient primingClient;
    private static ActivityClient activityClient;
    private static Scassandra scassandra;


    @BeforeClass
    public static void startScassandraServer() throws Exception {
        scassandra = ScassandraFactory.createServer();
        scassandra.start();
        primingClient = scassandra.primingClient();
        activityClient = scassandra.activityClient();
    }

    @AfterClass
    public static void shutdown() {
        scassandra.stop();
    }

    @Before
    public void setup() {
        underTest = new PersonDao(DEFAULT_SCASSANDRA_BINARY_PORT);
        activityClient.clearAllRecordedActivity();
        primingClient.clearAllPrimes();
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
        Query expectedQuery = Query.builder().withQuery("select * from person")
                .withConsistency("QUORUM").build();
        underTest.connect();
        activityClient.clearAllRecordedActivity();
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
        assertEquals(Arrays.asList("Christopher", "29"), executions.get(0).getVariables());
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

}

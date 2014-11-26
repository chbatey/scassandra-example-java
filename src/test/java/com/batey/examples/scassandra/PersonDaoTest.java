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
import org.scassandra.http.client.PrimingRequest.Result;
import org.scassandra.junit.ScassandraServerRule;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.scassandra.matchers.Matchers.*;

public class PersonDaoTest {

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule();
    public static final int CONFIGURED_RETRIES = 3;

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    private static final PrimingClient primingClient = SCASSANDRA.primingClient();
    private static final ActivityClient activityClient = SCASSANDRA.activityClient();

    private PersonDaoCassandra underTest;

    @Before
    public void setup() {
        underTest = new PersonDaoCassandra(8042, CONFIGURED_RETRIES);
        underTest.connect();
        activityClient.clearAllRecordedActivity();
    }

    @After
    public void after() {
        underTest.disconnect();
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
    public void testRetrievingOfNames() throws Exception {
        // given
        Map<String, ?> row = ImmutableMap.of(
                "first_name", "Chris",
                "last_name", "Batey",
                "age", 29);
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "age", ColumnTypes.Int
        );
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withColumnTypes(columnTypes)
                .withRows(row)
                .build());

        //when
        List<Person> names = underTest.retrievePeople();

        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0).getName());
    }

    @Test(expected = UnableToRetrievePeopleException.class)
    public void testHandlingOfReadRequestTimeout() throws Exception {
        // given
        PrimingRequest primeReadRequestTimeout = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withResult(Result.read_request_timeout)
                .build();
        primingClient.prime(primeReadRequestTimeout);

        //when
        underTest.retrievePeople();

        //then
    }

    @Test
    public void testCorrectQueryIssuedOnConnect() {
        //given
        Query expectedQuery = Query.builder().withQuery("USE people").withConsistency("ONE").build();

        //when
        underTest.connect();

        //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue("Expected query not executed, actual queries:  " + queries, queries.contains(expectedQuery));
    }

    @Test
    public void testCorrectQueryIssuedOnConnectUsingMatcher() {
        //given
        Query expectedQuery = Query.builder().withQuery("USE people").withConsistency("ONE").build();

        //when
        underTest.connect();

        //then
        assertThat(activityClient.retrieveQueries(), containsQuery(expectedQuery));
    }

    @Test
    public void testQueryIssuedWithCorrectConsistency() {
        //given
        Query expectedQuery = Query.builder().withQuery("select * from person").withConsistency("QUORUM").build();

        //when
        underTest.retrievePeople();

         //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue("Expected query with consistency QUORUM, found following queries: " + queries,
                queries.contains(expectedQuery));
    }

    @Test
    public void testQueryIssuedWithCorrectConsistencyUsingMatcher() {
        //given
        Query expectedQuery = Query.builder()
                .withQuery("select * from person")
                .withConsistency("QUORUM").build();

        //when
        underTest.retrievePeople();

        //then
        assertThat(activityClient.retrieveQueries(), containsQuery(expectedQuery));
    }

    @Test
    public void testStorePerson() {
        // given
        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("insert into person(first_name, age) values (?,?)")
                .withVariableTypes(ColumnTypes.Varchar, ColumnTypes.Int)
                .build();
        primingClient.prime(preparedStatementPrime);
        underTest.connect();

        //when
        underTest.storePerson(new Person("Christopher", 29));

        //then
        PreparedStatementExecution expectedPreparedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText("insert into person(first_name, age) values (?,?)")
                .withConsistency("ONE")
                .withVariables("Christopher", 29)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedPreparedStatement));
    }

    @Test
    public void testRetrievePeopleViaPreparedStatement() {
        // given
        Map<String, ?> row = ImmutableMap.of(
                "first_name", "Chris",
                "last_name", "Batey",
                "age", 29);
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of("age", ColumnTypes.Int);
        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from person where first_name = ?")
                .withVariableTypes(ColumnTypes.Varchar)
                .withColumnTypes(columnTypes)
                .withRows(row)
                .build();
        primingClient.prime(preparedStatementPrime);

        //when
        List<Person> names = underTest.retrievePeopleByName("Chris");

        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0).getName());
    }

    @Test
    public void testRetriesConfiguredNumberOfTimes() throws Exception {
        PrimingRequest readTimeoutPrime = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withResult(Result.read_request_timeout)
                .build();
        primingClient.prime(readTimeoutPrime);

        try {
            underTest.retrievePeople();
        } catch (UnableToRetrievePeopleException e) {
        }

        assertEquals(CONFIGURED_RETRIES + 1, activityClient.retrieveQueries().size());
    }

    @Test(expected = UnableToSavePersonException.class)
    public void testThatSlowQueriesTimeout() throws Exception {
        // given
        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("insert into person(first_name, age) values (?,?)")
                .withVariableTypes(ColumnTypes.Varchar, ColumnTypes.Int)
                .withFixedDelay(1000)
                .build();
        primingClient.prime(preparedStatementPrime);
        underTest.connect();

        underTest.storePerson(new Person("Christopher", 29));
    }

    @Ignore("Can't do this until scassandra sends back the consistency that is sent in")
    @Test
    public void testLowersConsistency() throws Exception {
        PrimingRequest readtimeoutPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from person")
                .withResult(Result.read_request_timeout)
                .build();
        primingClient.prime(readtimeoutPrime);

        try {
            underTest.retrievePeople();
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

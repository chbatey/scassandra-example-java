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
import com.google.common.collect.Sets;
import org.junit.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.*;
import org.scassandra.junit.ScassandraServerRule;

import java.util.*;

import static org.junit.Assert.*;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.set;
import static org.scassandra.http.client.PrimingRequest.Result.read_request_timeout;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;
import static org.scassandra.matchers.Matchers.containsQuery;
import static org.scassandra.matchers.Matchers.preparedStatementRecorded;

public class PersonDaoCassandraTest {

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule();

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    public static final int CONFIGURED_RETRIES = 1;

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
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withThen(then()
                    .withColumnTypes(column("age", PrimitiveType.INT), column("interesting_dates", set(TIMESTAMP)))
                    .withRows(row))
                .build());

        //when
        List<Person> names = underTest.retrievePeople();

        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0).getFirstName());
    }

    @Test(expected = UnableToRetrievePeopleException.class)
    public void testHandlingOfReadRequestTimeout() throws Exception {
        // given
        PrimingRequest primeReadRequestTimeout = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withThen(then().withResult(read_request_timeout))
                .build();
        primingClient.prime(primeReadRequestTimeout);

        //when
        underTest.retrievePeople();

        //then
    }

    @Test
    public void testCorrectQueryIssuedOnConnect() {
        //given
        Query expectedQuery = Query.builder().withQuery("USE scassandra").withConsistency("ONE").build();

        //when
        underTest.connect();

        //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue("Expected query not executed, actual queries:  " + queries, queries.contains(expectedQuery));
    }

    @Test
    public void testCorrectQueryIssuedOnConnectUsingMatcher() {
        //given
        Query expectedQuery = Query.builder().withQuery("USE scassandra").withConsistency("ONE").build();

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
                .withQueryPattern(".*person.*")
                .withThen(then().withVariableTypes(VARCHAR, VARCHAR, INT, set(TIMESTAMP)))
                .build();
        primingClient.prime(preparedStatementPrime);
        underTest.connect();
        Date interestingDate = new Date();
        Set<Date> interestingDates = Sets.newHashSet(interestingDate);

        //when
        underTest.storePerson(new Person("Christopher", "Batey", 29, interestingDates));

        //then
        PreparedStatementExecution expectedPreparedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText("insert into person(first_name, last_name, age, interesting_dates) values (?,?,?,?)")
                .withConsistency("ONE")
                .withVariables("Christopher", "Batey", 29, Sets.newHashSet(interestingDate))
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedPreparedStatement));
    }

    @Test
    public void testRetrievePeopleViaPreparedStatement() {
        // given
        Date today = new Date();
        Map<String, ?> row = ImmutableMap.of(
                "first_name", "Chris",
                "last_name", "Batey",
                "age", 29,
                "interesting_dates", Sets.newHashSet(today.getTime())
                );
        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from person where first_name = ? and last_name = ?")
                .withThen(then().withVariableTypes(VARCHAR, VARCHAR)
                .withColumnTypes(column("age", INT), column("interesting_dates", set(TIMESTAMP)))
                .withRows(row)).build();
        primingClient.prime(preparedStatementPrime);

        //when
        List<Person> names = underTest.retrievePeopleByName("Chris", "Batey");

        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0).getFirstName());
        assertEquals("Batey", names.get(0).getLastName());
        assertEquals(29, names.get(0).getAge());
        assertEquals(Sets.newHashSet(today), names.get(0).getInterestingDates());
    }

    @Test
    public void testRetriesConfiguredNumberOfTimes() throws Exception {
        PrimingRequest readTimeoutPrime = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withThen(then().withResult(read_request_timeout))
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
                .withQueryPattern("insert into person.*")
                .withThen(then()
                    .withVariableTypes(VARCHAR, VARCHAR, INT, set(TIMESTAMP))
                    .withFixedDelay(1000L))
                .build();
        primingClient.prime(preparedStatementPrime);
        underTest.connect();

        underTest.storePerson(new Person("Christopher", "Batey", 29, Collections.emptySet()));
    }

    @Test
    public void testLowersConsistency() throws Exception {
        PrimingRequest readtimeoutPrime = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withThen(then().withResult(read_request_timeout))
                .build();
        primingClient.prime(readtimeoutPrime);

        try {
            underTest.retrievePeople();
        } catch (UnableToRetrievePeopleException e) {
        }

        List<Query> queries = activityClient.retrieveQueries();
        assertEquals("Expected 2 attempts. Queries were: " + queries, 2, queries.size());
        assertEquals(Query.builder()
                .withQuery("select * from person")
                .withConsistency("QUORUM").build(), queries.get(0));
        assertEquals(Query.builder()
                .withQuery("select * from person")
                .withConsistency("ONE").build(), queries.get(1));
    }
}

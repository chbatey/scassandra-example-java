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
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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
        Map<String, String> row = ImmutableMap.of("first_name", "Chris");
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withRows(row).build();
        primingClient.primeQuery(pr);
        underTest.connect();

        //when
        List<String> names = underTest.retrieveNames();

        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0));
    }


    @Test(expected = UnableToRetrievePeopleException.class)
    public void testHandlingOfReadRequestTimeout() throws Exception {
        // given
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from person")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        primingClient.primeQuery(pr);

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

}

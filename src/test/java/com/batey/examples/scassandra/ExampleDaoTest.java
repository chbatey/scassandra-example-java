package com.batey.examples.scassandra;

import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ExampleDaoTest {

    private static int portNumber = 8042;
    private static int primingPortNumber = 8043;

    private PrimingClient primingClient;
    private ActivityClient activityClient;
    private static Scassandra scassandra;

    private ExampleDao underTest;

    @BeforeClass
    public static void startScassandraServerStub() throws Exception {
        scassandra = ScassandraFactory.createServer(portNumber, primingPortNumber);
        scassandra.start();
        Thread.sleep(2000);
    }

    @AfterClass
    public static void shutdown() {
        scassandra.stop();
    }

    @Before
    public void setup() {
        this.primingClient = new PrimingClient("localhost", primingPortNumber);
        this.activityClient = new ActivityClient("localhost", primingPortNumber);

        this.underTest = new ExampleDao(portNumber);

        this.activityClient.clearConnections();
        this.activityClient.clearQueries();
    }

    @After
    public void after() {
        this.underTest.disconnect();
    }

    @Test
    public void testRetrievingOfNames() throws Exception{
        // given
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(row).build();
        primingClient.primeQuery(pr);

        //when
        underTest.connect();
        List<String> names = underTest.retrieveNames();

        //then
        assertEquals(1, names.size());
        assertEquals("Chris", names.get(0));
    }


    @Test(expected = ExampleDaoException.class)
    public void testHandlingOfReadRequestTimeout() throws Exception {
        // given
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        primingClient.primeQuery(pr);

        //when
        underTest.connect();
        underTest.retrieveNames();

        //then
    }

    @Test
    public void testDaoConnectsToCassandra() {
        //given
        activityClient.clearConnections();
        //when
        underTest.connect();
        //then
        assertTrue(activityClient.retrieveConnections().size() > 0);
    }


    @Test
    public void testCorrectQueryIssuedOnConnect() {
        //given
        Query expectedQuery = new Query("use people", "ONE");
        //when
        underTest.connect();
        //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue(queries.contains(expectedQuery));
    }

    @Test
    public void testQueryIssuedWithCorrectConsistency() {
        //given
        Query expectedQuery = new Query("select * from people", "TWO");
        underTest.connect();
        //when
        underTest.retrieveNames();
        //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue("Expected query with consistency TWO, found following queries: " + queries,
                queries.contains(expectedQuery));
    }

}

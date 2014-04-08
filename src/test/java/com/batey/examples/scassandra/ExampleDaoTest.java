package com.batey.examples.scassandra;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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

    private ExampleDao underTest;
    private static Scassandra scassandra;

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


    @Test
    public void testRetrievingOfNames() throws Exception{
        // given
        Map<String, String> row = new HashMap<>();
        String primedName = "Chris";
        row.put("name", primedName);
        primingClient.prime(new PrimingRequest("select * from people", Arrays.asList(row)));

        //when
        underTest.connect();
        List<String> names = underTest.retrieveNames();

        //then
        assertEquals(1, names.size());
        assertEquals(primedName, names.get(0));
    }


    @Test(expected = ExampleDaoException.class)
    public void testHandlingOfReadRequestTimeout() throws Exception{
        // given
        primingClient.prime(new PrimingRequest("select * from people", PrimingRequest.Result.read_request_timeout));

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
        //when
        underTest.connect();
        //then
        List<Query> queries = activityClient.retrieveQueries();
        assertTrue(queries.stream().anyMatch(
                query -> query.getQuery().equals("use people")
        ));
    }

}

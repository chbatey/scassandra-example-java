package com.batey.examples.scassandra;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import uk.co.scassandra.ServerStubRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ExampleDaoTest {

    private static int portNumber = 8042;
    private static int primingPortNumber = 8043;
    private static final ServerStubRunner serverStub = new ServerStubRunner(portNumber, primingPortNumber);

    private PrimingClient primingClient = new PrimingClient("localhost", primingPortNumber);

    @BeforeClass
    public static void setup() throws Exception {
        new Thread() {
            public void run() {
                serverStub.start();
            }
        }.start();
        Thread.sleep(3000);
        System.out.println("Server stub started");
    }

    @AfterClass
    public static void shutdown() {
        serverStub.shutdown();
    }

    @Test
    public void testRetrievingOfNames() throws Exception{
        // given
        Map<String, String> row = new HashMap<>();
        String primedName = "Chris";
        row.put("name", primedName);
        primingClient.prime(new PrimingRequest("select * from people", Arrays.asList(row)));
        ExampleDao exampleDao = new ExampleDao(portNumber);

        //when
        exampleDao.connect();
        List<String> names = exampleDao.retrieveNames();

        //then
        assertEquals(1, names.size());
        assertEquals(primedName, names.get(0));
    }


    @Test(expected = ExampleDaoException.class)
    public void testHandlingOfReadRequestTimeout() throws Exception{
        // given
        primingClient.prime(new PrimingRequest("select * from people", PrimingRequest.Result.read_request_timeout));
        ExampleDao exampleDao = new ExampleDao(portNumber);

        //when
        exampleDao.connect();
        exampleDao.retrieveNames();

        //then

    }
}

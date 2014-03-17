package com.batey.examples.scassandra;

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.*;
import uk.co.scassandra.ServerStubRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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
    public void test() throws Exception{
        Map<String, String> row = new HashMap<>();
        row.put("name", "Chris");
        primingClient.prime(new PrimingRequest("select * from people", Arrays.asList(row)));

        ExampleDao exampleDao = new ExampleDao(portNumber);
        exampleDao.connect();
        List<String> names = exampleDao.retrieveNames();
        assertEquals(1, names.size());
    }

    private static class PrimingClient {
        private String host;
        private int port;
        private Gson gson = new Gson();

        public PrimingClient(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public void prime(PrimingRequest primeRequest) throws PrimeFailedException {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost("http://"+host+":" + port + "/prime");
            String jsonAsString = gson.toJson(primeRequest);
            httpPost.setEntity(new StringEntity(jsonAsString, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response1 = httpClient.execute(httpPost);
                if (response1.getStatusLine().getStatusCode() != 200) throw new PrimeFailedException();
            } catch (IOException e) {
                throw new PrimeFailedException();
            }

        }
    }

    private static class PrimeFailedException extends RuntimeException {}

    private static class PrimingRequest {
        private String when;
        private List<Map<String, String>> then;

        private PrimingRequest(String when, List<Map<String, String>> rows) {
            this.when = when;
            this.then = rows;
        }
    }
}

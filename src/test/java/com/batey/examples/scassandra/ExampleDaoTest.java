package com.batey.examples.scassandra;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.*;
import uk.co.scassandra.ServerStubRunner;
import java.util.List;

import static org.junit.Assert.*;

public class ExampleDaoTest {

    private static int portNumber = 8042;
    private static int primingPortNumber = 8043;
    private static final ServerStubRunner serverStub = new ServerStubRunner(portNumber, primingPortNumber);

    @BeforeClass
    public static void setup() throws Exception {
        new Thread() {
            public void run() {
                serverStub.start();
            }
        }.start();
        Thread.sleep(3000);
    }

    @AfterClass
    public static void shutdown() {

        serverStub.shutdown();
    }


    @Test
    public void test() throws Exception{
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost("http://localhost:8043/prime");
        String primeRequest = "{\"when\":\"select * from people\", \"then\": [{\"name\":\"Chris\"}] }";
        httpPost.setEntity(new ByteArrayEntity(primeRequest.getBytes()));
        httpPost.setHeader("Content-Type", "application/json");
        CloseableHttpResponse response1 = httpclient.execute(httpPost);
        assertEquals(response1.getStatusLine().getStatusCode(), 200);

        ExampleDao exampleDao = new ExampleDao(8042);
        exampleDao.connect();
        List<String> names = exampleDao.retrieveNames();
        assertEquals(1, names.size());
    }
}

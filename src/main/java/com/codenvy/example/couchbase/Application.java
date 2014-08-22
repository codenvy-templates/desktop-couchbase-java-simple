package com.codenvy.example.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.google.gson.Gson;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application {
    public static void main(String[] args) throws Exception {
        List<URI> hosts = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));

        final String bucket = "test_bucket";
        final String password = "";

        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();

        CouchbaseClient client = new CouchbaseClient(cfb.buildCouchbaseConnection(hosts, bucket, password));

        final String newDocumentKey = client.set("my-first-document", "Hello Couchbase!").getKey();

        System.out.println(String.format("New document created with key: %s, and value: %s", newDocumentKey, client.get(newDocumentKey)));

        client.delete(newDocumentKey);

        Gson gson = new Gson();

        User user1 = new User("John", "Doe");
        User user2 = new User("Matt", "Ingenthron");
        User user3 = new User("Carol", "Nitschinger");

        List<String> insertedKeys = new ArrayList<>();

        insertedKeys.add(client.set("user1", gson.toJson(user1)).getKey());
        insertedKeys.add(client.set("user2", gson.toJson(user2)).getKey());
        insertedKeys.add(client.set("user3", gson.toJson(user3)).getKey());

        for (String insertedKey : insertedKeys) {
            System.out.println(String.format("Inserted object with key: %s, and value: %s", insertedKey, client.get(insertedKey)));
        }

        client.shutdown();
    }
}

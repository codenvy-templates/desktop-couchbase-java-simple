package com.codenvy.example.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.google.gson.Gson;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application {
    public static void main(String[] args) throws Exception {
        List<URI> hosts = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));

        final String bucket = "default";
        final String password = "";

        System.setProperty("viewmode", "development");

        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        cfb.setViewTimeout(10000);

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

        DesignDocument designDoc = new DesignDocument("dev_users");

        final String viewName = "by_firstName";
        final String mapFunction =
                "function (doc, meta) {\n" +
                "  if(doc.firstName) {\n" +
                "    emit(doc.firstName, null);\n" +
                "  }\n" +
                "}";

        ViewDesign viewDesign = new ViewDesign(viewName, mapFunction);
        designDoc.getViews().add(viewDesign);

        client.createDesignDoc(designDoc);

        View view = client.getView("dev_users", "by_firstName");

        System.out.println("Creating view to filter users firstName which starts from 'M' char");

        Query query = new Query();
        query.setIncludeDocs(true);
        query.setRangeStart("M");
        query.setRangeEnd("M\\uefff"); // Stop before N starts
        query.setStale(Stale.FALSE);

        ViewResponse viewResponse = client.query(view, query);

        for (ViewRow row : viewResponse) {
            System.out.println(String.format("Found: %s", row.getDocument()));
        }

        client.shutdown();
    }
}

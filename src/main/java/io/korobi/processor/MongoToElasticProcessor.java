package io.korobi.processor;

import org.bson.BsonDateTime;
import org.bson.Document;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import javax.inject.Inject;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

public class MongoToElasticProcessor implements IDocumentProcessor {

    private Logger logger;
    private Client esClient;

    @Inject
    public MongoToElasticProcessor(Logger logger, Client esClient) {
        this.logger = logger;
        this.esClient = esClient;
    }

    public void run(List<Document> docsToProcess) {
        BulkRequestBuilder bulkRequest = esClient.prepareBulk();

        for (Document doc : docsToProcess) {
            doc.put("mongoId", doc.get("_id").toString());
            doc.remove("_id");
            Date date = (Date) doc.get("date");
            doc.put("date", date.getTime());
            doc.remove("imported");
            bulkRequest.add(esClient.prepareIndex("chats", "chat").setSource(doc.toJson()));
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            logger.severe("Bulk response failure! (this is bad) " + bulkResponse.getItems()[0].getFailureMessage());
        } else {
            logger.info("Thread is complete.");
        }

    }
}

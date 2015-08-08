package io.korobi.processor;

import org.bson.BsonDateTime;
import org.bson.Document;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import javax.inject.Inject;
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
            doc.put("_id", doc.get("_id").toString());
            BsonDateTime date = (BsonDateTime) doc.get("date");
            doc.put("date", date.getValue());
            doc.remove("imported");
            bulkRequest.add(esClient.prepareIndex("chats", "chat").setSource(doc.toJson()));
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            logger.severe("Bulk response failure! (this is bad)");
        } else {
            logger.info("Thread is complete.");
        }

    }
}

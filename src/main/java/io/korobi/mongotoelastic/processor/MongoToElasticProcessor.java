package io.korobi.mongotoelastic.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

public class MongoToElasticProcessor implements IDocumentProcessor {

    private static Logger logger = LogManager.getLogger();
    private Client esClient;

    @Inject
    public MongoToElasticProcessor(Client esClient) {
        this.esClient = esClient;
    }

    @Override
    public void run(List<Document> documents) {
        BulkRequestBuilder bulkRequest = this.esClient.prepareBulk();

        for (Document doc : documents) {
            try {
                Date date = (Date) doc.get("date");
                XContentBuilder docBuilder =
                    XContentFactory.jsonBuilder()
                        .startObject()
                            .field("actor_hostname", (String) doc.get("actor_hostname"))
                            .field("actor_name", (String) doc.get("actor_name"))
                            .field("actor_prefix", (String) doc.get("actor_prefix"))
                            .field("channel", (String) doc.get("channel"))
                            // .field("channel_mode", (String) doc.get("channel_mode")) // TODO: Check w/ kashike
                            .field("message", (String) doc.get("message"))
                            .field("network", (String) doc.get("network"))
                            .field("recipient_hostname", (String) doc.get("recipient_hostname"))
                            .field("recipient_name", (String) doc.get("recipient_name"))
                            .field("recipient_prefix", (String) doc.get("recipient_prefix"))
                            .field("type", (String) doc.get("type"))
                            .field("mongoId", doc.get("_id").toString())
                            .field("date", date.getTime())
                        .endObject();
                bulkRequest.add(this.esClient.prepareIndex("chats", "chat").setSource(docBuilder));
                logger.info(docBuilder.string());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            logger.error("Bulk response failure! (this is bad) " + bulkResponse.getItems()[0].getFailureMessage());
        }
    }
}

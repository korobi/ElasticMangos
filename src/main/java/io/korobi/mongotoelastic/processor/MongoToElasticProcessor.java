package io.korobi.mongotoelastic.processor;

import io.korobi.mongotoelastic.exception.ImportException;
import io.korobi.mongotoelastic.logging.InjectLogger;
import io.korobi.mongotoelastic.mongo.IChannelBlacklist;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Date;
import java.util.List;

public class MongoToElasticProcessor implements IDocumentProcessor {

    @InjectLogger
    private Logger logger;
    private Client esClient;
    private IChannelBlacklist blacklist;

    @Inject
    public MongoToElasticProcessor(Client esClient, IChannelBlacklist blacklist) {
        this.esClient = esClient;
        this.blacklist = blacklist;
    }

    @Override
    public void run(List<Document> documents) {
        BulkRequestBuilder bulkRequest = this.esClient.prepareBulk();

        for (Document doc : documents) {
            if (this.blacklist.isBlacklisted(doc.getString("network"), doc.getString("channel"))) {
                continue;
            }
            
            try {
                Date date = (Date) doc.get("date");
                XContentBuilder docBuilder =
                    XContentFactory.jsonBuilder()
                        .startObject()
                            .field("actor_hostname", doc.getString("actor_hostname"))
                            .field("actor_name", doc.getString("actor_name"))
                            .field("actor_prefix", doc.getString("actor_prefix"))
                            .field("channel", doc.getString("channel"))
                            // .field("channel_mode", doc.getString("channel_mode")) // TODO: Check w/ kashike
                            .field("message", doc.getString("message"))
                            .field("network", doc.getString("network"))
                            .field("recipient_hostname", doc.getString("recipient_hostname"))
                            .field("recipient_name", doc.getString("recipient_name"))
                            .field("recipient_prefix", doc.getString("recipient_prefix"))
                            .field("type", doc.getString("type"))
                            .field("mongoId", doc.get("_id").toString())
                            .field("date", date.getTime())
                        .endObject();
                bulkRequest.add(this.esClient.prepareIndex("chats", "chat").setSource(docBuilder));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            logger.error("Bulk response failure! (this is bad) " + bulkResponse.getItems()[0].getFailureMessage());
            throw new ImportException(bulkResponse.getItems()[0].getFailureMessage());
        } else {
            logger.info(String.format("%d docs were imported.", documents.size()));
        }
    }
}

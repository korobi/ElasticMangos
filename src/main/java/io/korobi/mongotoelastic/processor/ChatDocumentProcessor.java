package io.korobi.mongotoelastic.processor;

import com.mongodb.client.MongoCursor;
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
import java.util.concurrent.atomic.AtomicInteger;

public class ChatDocumentProcessor implements IDocumentProcessor {

    @InjectLogger
    private Logger logger;
    private final Client esClient;
    private final IChannelBlacklist blacklist;
    private final AtomicInteger batchNo;

    @Inject
    public ChatDocumentProcessor(Client esClient, IChannelBlacklist blacklist) {
        this.esClient = esClient;
        this.blacklist = blacklist;
        this.batchNo = new AtomicInteger();
    }

    @Override
    public void run(MongoCursor<Document> documents, int bulkSize) {
        BulkRequestBuilder bulkRequest = this.esClient.prepareBulk();
        Document doc;
        int i = 0;
        int count = 0;
        int blCount = 0;

        synchronized(documents) {
            doc = documents.tryNext();
        }
        while(doc != null) {
            if (this.blacklist.isBlacklisted(doc.getString("network"), doc.getString("channel"))) {
                synchronized(documents) {
                    doc = documents.tryNext();
                }
                ++blCount;
                continue;
            }

            try {
                Date date = doc.getDate("date");
                XContentBuilder docBuilder =
                    XContentFactory.jsonBuilder()
                        .startObject()
                            .field("actor_hostname", doc.getString("actor_hostname"))
                            .field("actor_name", doc.getString("actor_name"))
                            .field("actor_prefix", doc.getString("actor_prefix"))
                            .field("channel", doc.getString("channel"))
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
                ++i;
                ++count;
            } catch (IOException e) {
                logger.error(e);
            }

            if(i >= bulkSize) {
                processBulkRequest(bulkRequest, blCount);
                bulkRequest = this.esClient.prepareBulk();
                i = 0;
            }

            synchronized(documents) {
                doc = documents.tryNext();
            }
        }

        processBulkRequest(bulkRequest, blCount);
        logger.info("Thread '{}' imported {} chat documents.", Thread.currentThread().getName(), count);
    }

    private void processBulkRequest(BulkRequestBuilder bulkRequest, int blCount) {
        if (bulkRequest.numberOfActions() == 0) {
            return;
        }

        int currentNo = batchNo.incrementAndGet();
        logger.info("Processing bulk request {}.", currentNo);

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            logger.error("Bulk response failure! (this is bad) Request: {}, message: {}", currentNo, bulkResponse.getItems()[0].getFailureMessage());
            throw new ImportException(bulkResponse.getItems()[0].getFailureMessage());
        } else {
            logger.info("{} docs were processed by request {} with blCount of {}.", bulkRequest.numberOfActions(), currentNo, blCount);
        }
    }

}

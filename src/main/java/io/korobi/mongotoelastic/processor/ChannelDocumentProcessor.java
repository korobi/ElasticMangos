package io.korobi.mongotoelastic.processor;

import com.mongodb.client.MongoCursor;
import io.korobi.mongotoelastic.exception.ImportException;
import io.korobi.mongotoelastic.logging.InjectLogger;
import io.korobi.mongotoelastic.mongo.IChannelBlacklist;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class ChannelDocumentProcessor implements IDocumentProcessor {

    @InjectLogger
    private Logger logger;
    private final Client esClient;
    private final IChannelBlacklist blacklist;

    @Inject
    public ChannelDocumentProcessor(Client esClient, IChannelBlacklist blacklist) {
        this.esClient = esClient;
        this.blacklist = blacklist;
    }

    @Override
    public void run(MongoCursor<Document> documents, int bulkSize) {
        throw new UnsupportedOperationException("No support for multithreaded channel import");
    }

    @Override
    public void run(MongoCursor<Document> documents) {
        // We don't have enough channels to necessitate processing in a multithreaded manner
        BulkRequestBuilder bulkRequest = this.esClient.prepareBulk();
        while (documents.hasNext()) {
            Document doc = documents.next();
            String network = doc.getString("network");
            String channel = doc.getString("channel");
            if (this.blacklist.isBlacklisted(network, channel)) {
                logger.info(String.format("Skipping channel %s on network %s due to blacklist.", channel, network));
                continue;
            }

            try {
                XContentBuilder builder = buildDocument(doc);
                if (builder != null) {
                    IndexRequestBuilder boohoo_zarthusLikedTheFirstRunOfEM = this.esClient.prepareIndex("channels", "channel")
                        .setId(doc.getObjectId("_id").toHexString())
                        .setSource(builder);
                    bulkRequest.add(boohoo_zarthusLikedTheFirstRunOfEM);
                }
            } catch (IOException e) {
                logger.error(e);
            }
        }
        logger.info("Submitting BR for {} channels", bulkRequest.numberOfActions());
        processBulkRequest(bulkRequest);
    }

    private void processBulkRequest(BulkRequestBuilder bulkRequest) {
        if (bulkRequest.numberOfActions() == 0) {
            return;
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            logger.error("Bulk response failure! (this is bad!) Message: {}", bulkResponse.getItems()[0].getFailureMessage());
            throw new ImportException(bulkResponse.getItems()[0].getFailureMessage());
        } else {
            logger.info("{} channels were processed by the bulk request.", bulkRequest.numberOfActions());
        }
    }

    private XContentBuilder buildDocument(Document doc) throws IOException {
        // The fun part about storing these two dates is that it means we
        // ideally need to reindex the channel object every time someone
        // joins/parts/quits.

        Date lastValidContentAt = doc.getDate("last_activity_valid");
        Date lastActivity = doc.getDate("last_activity");
        if (lastActivity == null || lastValidContentAt == null) {
            logger.warn("Refusing to index channel ({} on {}) with no activity.", doc.get("channel"), doc.get("network"));
            return null;
        }
        String channel = doc.getString("channel");
        String network = doc.getString("network");
        String mongoId = doc.get("_id").toString();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .field("mongoId", mongoId)
                .field("channel", channel)
                .field("network", network)
                .field("added_at", doc.getObjectId("_id").getDate().getTime())
                .field("last_activity_valid", lastValidContentAt.getTime())
                .field("last_activity", lastActivity.getTime());
                this.appendTopic(builder, doc.get("topic", Document.class));
                builder.startObject("_name_suggest")
                    .array("input", channel, String.format("%s:%s", network, channel))
                    .startObject("payload")
                        .field("network", network)
                        .field("channel", channel)
                        .field("mongoId", mongoId)
                    .endObject()
                    .endObject()
            .endObject();

        return builder;
    }

    private void appendTopic(XContentBuilder builder, Document topic) throws IOException {
        String host = topic.getString("actor_host");
        String nick = topic.getString("actor_nick");
        Date time = topic.getDate("time");
        String value = topic.getString("value");
        if (host != null && nick != null && time != null && value != null) {
            builder.startObject("topic")
                .field("actor_host", host)
                .field("actor_nick", nick)
                .field("time", time.getTime())
                .field("value", value)
                .endObject();
        }
    }

}

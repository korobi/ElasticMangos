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
        // We don't have enough channels to necessitate processing in a multithreaded manner
        while (documents.hasNext()) {
            Document doc = documents.next();
            String network = doc.getString("network");
            String channel = doc.getString("channel");
            if (this.blacklist.isBlacklisted(network, channel)) {
                logger.info(String.format("Skipping channel %s on network %s due to blacklist.", channel, network));
                continue;
            }

            try {
                buildDocument(doc);
            } catch (IOException e) {
                logger.error(e);
            }
        }
    }

    private XContentBuilder buildDocument(Document doc) throws IOException {
        // The fun part about storing these two dates is that it means we
        // ideally need to reindex the channel object every time someone
        // joins/parts/quits.

        Date lastValidContentAt = doc.getDate("last_valid_content_at");
        Date lastActivity = doc.getDate("last_activity");

        return XContentFactory.jsonBuilder()
                .startObject()
                    .field("mongoId", doc.get("_id").toString())
                    .field("channel", doc.getString("channel"))
                    .field("network", doc.getString("network"))
                    .field("network", doc.getString("network"))
                    .field("last_valid_content_at", lastValidContentAt.getTime())
                    .field("last_activity", lastActivity.getTime())
                    .startObject("topic")
                        // TODO: doc.getString("topic.actor_host") ??
                    .endObject()
                .endObject();
    }

}

package io.korobi.mongotoelastic.elasticsearch;

import io.korobi.mongotoelastic.logging.InjectLogger;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

import javax.inject.Inject;

public class IndexInitialiser {

    @InjectLogger
    private Logger logger;
    private final Client esClient;

    @Inject
    public IndexInitialiser(Client esClient) {
        this.esClient = esClient;
    }

    public void initialise() {
        IndicesAdminClient indices = this.esClient.admin().indices();
        removeIndices(indices);
        logger.info("Indexes removed.");

        createIndices(indices);
        logger.info("Created indices!");
    }

    private void createIndices(IndicesAdminClient indices) {
        createChatsIndex(indices);
        createChannelsIndex(indices);
    }

    private void createChatsIndex(IndicesAdminClient indices) {
        CreateIndexRequest createBuilder = new CreateIndexRequest("chats");
        try {
            // @formatter:off
            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("chat")
                        .startObject("properties")
                            .startObject("date")
                                .field("type", "long")
                            .endObject()
                            .startObject("type")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
            createBuilder.mapping("chat", mappingBuilder);
            // @formatter:on
        } catch (IOException e) {
            e.printStackTrace();
        }
        indices.create(createBuilder);
    }

    private void createChannelsIndex(IndicesAdminClient indices) {
        CreateIndexRequest createBuilder = new CreateIndexRequest("channels");
        try {
            // @formatter:off
            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("channel")
                            .startObject("properties")
                    .startObject("topic")
                                    .field("type", "object") // we only have one so don't use type 'nested'
                                .endObject()
                                .startObject("topic.time")
                                    .field("type", "long")
                                .endObject()
                                .startObject("last_activity")
                                    .field("type", "long")
                                .endObject()
                                .startObject("added_at")
                                    .field("type", "long")
                                .endObject()
                                .startObject("last_valid_content_at")
                                    .field("type", "long")
                                .endObject()
                                .startObject("_name_suggest")
                                    .field("payloads", true)
                                    .field("index_analyzer", "simple")
                                    .field("search_analyzer", "simple")
                                    .field("type", "completion")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject();
            createBuilder.mapping("channel", mappingBuilder);
            // @formatter:on
        } catch (IOException e) {
            e.printStackTrace();
        }
        indices.create(createBuilder);
    }

    private void removeIndices(IndicesAdminClient indices) {
        DeleteIndexRequestBuilder chatsDeleteBuilder = new DeleteIndexRequestBuilder(indices, "chats");
        DeleteIndexRequestBuilder channelsDeleteBuilder = new DeleteIndexRequestBuilder(indices, "channels");
        indices.delete(chatsDeleteBuilder.request());
        indices.delete(channelsDeleteBuilder.request());
    }
}

package io.korobi.mongotoelastic.elasticsearch;

import org.apache.logging.log4j.LogManager;
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

    private static final Logger logger = LogManager.getLogger();
    private final Client esClient;

    @Inject
    public IndexInitialiser(Client esClient) {
        this.esClient = esClient;
    }

    public void initialise() {
        IndicesAdminClient indices = this.esClient.admin().indices();
        DeleteIndexRequestBuilder deleteBuilder = new DeleteIndexRequestBuilder(indices, "chats");
        indices.delete(deleteBuilder.request());
        logger.info("Indexes removed.");

        CreateIndexRequest createBuilder = new CreateIndexRequest("chats");
        try {
            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("chat")
                        .startObject("properties")
                            .startObject("date")
                                .field("type", "long")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
            createBuilder.mapping("chat", mappingBuilder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Created index!");
        indices.create(createBuilder);
    }
}

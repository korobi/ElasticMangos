package io.korobi.elasticsearch;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;

import javax.inject.Inject;
import java.util.logging.Logger;

public class IndexInitialiser {

    private final Logger logger;
    private final Client esClient;

    @Inject
    public IndexInitialiser(Logger logger, Client esClient) {
        this.logger = logger;
        this.esClient = esClient;
    }

    public void initialise() {
        IndicesAdminClient indices = esClient.admin().indices();
        DeleteIndexRequestBuilder deleteBuilder = new DeleteIndexRequestBuilder(indices, "chats");
        indices.delete(deleteBuilder.request());
        logger.info("Indexes removed.");

        CreateIndexRequest createBuilder = new CreateIndexRequest("chats");
        // TODO
        indices.create(createBuilder);
    }
}

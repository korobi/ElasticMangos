package io.korobi.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import javax.inject.Inject;
import java.util.logging.Logger;

public class MongoRetriever {

    @Inject
    public MongoRetriever(MongoDatabase database, Logger logger) {
        logger.info(String.valueOf(database.getCollection("chats").count()));

    }
}

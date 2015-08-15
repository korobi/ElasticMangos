package io.korobi.mongotoelastic.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.korobi.mongotoelastic.logging.InjectLogger;
import io.korobi.mongotoelastic.opt.IOptions;
import io.korobi.mongotoelastic.processor.ChannelDocumentProcessor;
import io.korobi.mongotoelastic.processor.IDocumentProcessor;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import javax.inject.Inject;

public class MongoChannelRetriever {

    @InjectLogger
    private Logger logger;
    private final MongoDatabase database;
    private final IOptions opts;
    private final IDocumentProcessor processor;

    @Inject
    public MongoChannelRetriever(MongoDatabase database, IOptions opts, ChannelDocumentProcessor processor) {
        this.database = database;
        this.opts = opts;
        this.processor = processor;
    }

    public void processData() {
        MongoCollection<Document> collection = this.database.getCollection("channels");
        logger.info("There are " + String.valueOf(collection.count()) + " channels!");

        FindIterable<Document> iterable = collection.find();
        iterable.batchSize(this.opts.getBatchSize());

        MongoCursor<Document> cursor = iterable.iterator();

        this.processor.run(cursor);
        cursor.close();
    }

}

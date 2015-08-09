package io.korobi.mongotoelastic.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.korobi.mongotoelastic.logging.InjectLogger;
import io.korobi.mongotoelastic.opt.IOptions;
import io.korobi.mongotoelastic.processor.IDocumentProcessor;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MongoRetriever {

    @InjectLogger
    private Logger logger;
    private final MongoDatabase database;
    private final IOptions opts;
    private final IDocumentProcessor processor;
    private final int itemsPerThread;
    private final ExecutorService threadPool;

    @Inject
    public MongoRetriever(MongoDatabase database, IOptions opts, IDocumentProcessor processor) {
        this.database = database;
        this.opts = opts;
        this.processor = processor;
        this.itemsPerThread = opts.getBatchSize();
        this.threadPool = Executors.newFixedThreadPool(opts.getThreadCap());
    }

    public void processData() {
        MongoCollection<Document> collection = this.database.getCollection("chats");
        logger.info("There are " + String.valueOf(collection.count()) + " chats!");

        FindIterable<Document> iterable = collection.find();
        iterable.batchSize(this.opts.getBatchSize());
        MongoCursor<Document> cursor = iterable.iterator();

        List<Document> documents = new ArrayList<>(this.itemsPerThread);
        int current = 0;
        while(cursor.hasNext()) {
            if(current < this.itemsPerThread) {
                ++current;
                documents.add(cursor.next());
            } else {
                logger.info(String.format("Got a new batch of %d documents", documents.size()));
                processBatch(documents);
                documents = new ArrayList<>(this.itemsPerThread);
                current = 0;
            }
        }
        cursor.close();

        if(!documents.isEmpty()) {
            logger.info("Last batch!");
            processBatch(documents);
        }

        this.threadPool.shutdown();
        try {
            this.threadPool.awaitTermination(0, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processBatch(List<Document> documents) {
        this.threadPool.submit(() -> this.processor.run(documents));
    }

}

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
        int batches = (int) Math.ceil(collection.count() / (double) opts.getBatchSize());
        FindIterable<Document> iterable = collection.find();
        iterable.batchSize(this.opts.getBatchSize());
        MongoCursor<Document> cursor = iterable.iterator();

        List<Document> documents = new ArrayList<>(this.itemsPerThread);
        int current = 0;
        int batchNo = 1;
        while(cursor.hasNext()) {
            if(current < this.itemsPerThread) {
                ++current;
                documents.add(cursor.next());
            } else {
                logger.info(String.format("Got a new batch of %d documents. %d of %d batches.", documents.size(), batchNo, batches));
                processBatch(documents, batchNo);
                documents = new ArrayList<>(this.itemsPerThread);
                current = 0;
                batchNo++;
            }
        }
        cursor.close();

        if(!documents.isEmpty()) {
            logger.info("Last batch!");
            processBatch(documents, batchNo);
        }

        this.threadPool.shutdown();
        try {
            if(!this.threadPool.awaitTermination(0, TimeUnit.MILLISECONDS)) {
                logger.warn("Thread pool did not terminate");
            }
        } catch(InterruptedException e) {
            logger.error(e);
        }
    }

    private void processBatch(List<Document> documents, int batchNo) {
        this.threadPool.submit(() -> {
            logger.info(String.format("Handling batch %d.", batchNo));
            this.processor.run(documents);
        });
    }

}

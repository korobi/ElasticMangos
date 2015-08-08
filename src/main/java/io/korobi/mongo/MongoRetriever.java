package io.korobi.mongo;

import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.korobi.exceptions.UserIsAtFaultException;
import io.korobi.opts.IOptions;
import io.korobi.processor.IDocumentProcessor;
import io.korobi.processor.RunnableProcessor;
import org.bson.Document;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class MongoRetriever {

    private final MongoDatabase database;
    private IOptions opts;
    private final Logger logger;
    private IDocumentProcessor processor;
    private final int itemsPerThread;

    @Inject
    public MongoRetriever(MongoDatabase database, IOptions opts, Logger logger, IDocumentProcessor processor) {
        this.database = database;
        this.opts = opts;
        this.logger = logger;
        this.processor = processor;
        double itemsPerThread = opts.getBatchSize() / (double) opts.getThreadCap();
        if (itemsPerThread != Math.ceil(itemsPerThread)) {
            throw new UserIsAtFaultException("Disallowing poor thread-related option choice.");
        }
        this.itemsPerThread = (int) itemsPerThread;
        // MongoClient & MongoDatabase are thread safe :)
        processData();
    }

    private void processData() {
        MongoCollection<Document> collection = database.getCollection("chats");
        logger.info("There are " + String.valueOf(collection.count()) + " chats!");

        List<Document> currentBatch;
        MongoCursor<Document> cursor = collection.find().iterator();
        logger.info("Done with that");
        while (!(currentBatch = buildBatch(cursor)).isEmpty()) {
            // great! We have a bunch of documents in RAM now :D
            CountDownLatch latch = new CountDownLatch(opts.getThreadCap());
            logger.info("Got a new batch of" + currentBatch.size() + " documents");
            for (int threadNumber = 1; threadNumber <= opts.getThreadCap(); threadNumber++) {
                int fromIndex = (threadNumber - 1) * itemsPerThread;
                List<Document> forThread = currentBatch.subList(fromIndex, fromIndex + itemsPerThread);
                Thread thread = new Thread(new RunnableProcessor(forThread, processor, latch));
                thread.start();
                logger.info(String.format("Spawned thread %d", threadNumber));
            }

            logger.info("Awaiting current batch end...");

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cursor.close();
    }

    private List<Document> buildBatch(MongoCursor<Document> cursor) {
        List<Document> currentBatch = new ArrayList<Document>(opts.getBatchSize());
        while (cursor.hasNext()) {
            logger.fine("ping");
            currentBatch.add(cursor.next()); // yield return cursor.next() :(
        }
        return currentBatch;
    }
}

package io.korobi.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.korobi.exceptions.UserIsAtFaultException;
import io.korobi.opts.IOptions;
import io.korobi.processor.IDocumentProcessor;
import io.korobi.processor.RunnableProcessor;
import io.korobi.utils.NumberUtil;
import org.bson.Document;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class MongoRetriever {

    private MongoDatabase database;
    private IOptions opts;
    private Logger logger;
    private IDocumentProcessor processor;
    private int itemsPerThread;

    @Inject
    public MongoRetriever(MongoDatabase database, IOptions opts, Logger logger, IDocumentProcessor processor) {
        this.database = database;
        this.opts = opts;
        this.logger = logger;
        this.processor = processor;

        checkUserThreadParameters();

        // MongoClient & MongoDatabase are thread safe :)
        processData();
    }

    private void checkUserThreadParameters() {
        double itemsPerThread = opts.getBatchSize() / (double) opts.getThreadCap();
        if (NumberUtil.isValueFractional(itemsPerThread)) {
            throw new UserIsAtFaultException("Disallowing poor thread-related option choice.");
        }
        this.itemsPerThread = (int) itemsPerThread;
    }

    private void processData() {
        MongoCollection<Document> collection = database.getCollection("chats");
        logger.info("There are " + String.valueOf(collection.count()) + " chats!");

        List<Document> currentBatch;
        FindIterable<Document> iterable = collection.find();
        iterable.batchSize(opts.getBatchSize());
        MongoCursor<Document> cursor = iterable.iterator();
        logger.info("Done with that");
        while (!(currentBatch = buildBatch(cursor)).isEmpty()) {
            // great! We have a bunch of documents in RAM now :D
            CountDownLatch latch = new CountDownLatch(opts.getThreadCap());
            logger.info(String.format("Got a new batch of %d documents", currentBatch.size()));
            if (currentBatch.size() != opts.getBatchSize()) {
                logger.info("Last batch!");
            }

            for (int threadNumber = 1; threadNumber <= opts.getThreadCap(); threadNumber++) {
                int fromIndex = (threadNumber - 1) * itemsPerThread;
                int endIndex = fromIndex + itemsPerThread;
                int lastIndexInBatch = currentBatch.size() - 1;
                if (endIndex > lastIndexInBatch) {
                    endIndex = lastIndexInBatch;
                }
                List<Document> forThread = currentBatch.subList(fromIndex, endIndex);
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
        logger.info("Hello from buildBatch!");
        List<Document> currentBatch = new ArrayList<Document>(opts.getBatchSize());
        logger.info("Instantiated ArrayList :)");

        while (cursor.hasNext() && currentBatch.size() < opts.getBatchSize()) {
            currentBatch.add(cursor.next()); // yield return cursor.next() :(
        }
        return currentBatch;
    }
}

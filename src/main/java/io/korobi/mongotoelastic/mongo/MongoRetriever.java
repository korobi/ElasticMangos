package io.korobi.mongotoelastic.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.korobi.mongotoelastic.exception.UserIsAtFaultException;
import io.korobi.mongotoelastic.opt.IOptions;
import io.korobi.mongotoelastic.processor.IDocumentProcessor;
import io.korobi.mongotoelastic.processor.RunnableProcessor;
import io.korobi.mongotoelastic.util.NumberUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.inject.Inject;

public class MongoRetriever {

    private static Logger logger = LogManager.getLogger();
    private MongoDatabase database;
    private IOptions opts;
    private IDocumentProcessor processor;
    private int itemsPerThread;

    @Inject
    public MongoRetriever(MongoDatabase database, IOptions opts, IDocumentProcessor processor) {
        this.database = database;
        this.opts = opts;
        this.processor = processor;

        this.checkUserThreadParameters();

        // MongoClient & MongoDatabase are thread safe :)
        this.processData();
    }

    private void checkUserThreadParameters() {
        double itemsPerThread = this.opts.getBatchSize() / (double) this.opts.getThreadCap();
        if (NumberUtil.isValueFractional(itemsPerThread)) {
            throw new UserIsAtFaultException("Disallowing poor thread-related option choice.");
        }
        this.itemsPerThread = (int) itemsPerThread;
    }

    private void processData() {
        MongoCollection<Document> collection = this.database.getCollection("chats");
        logger.info("There are " + String.valueOf(collection.count()) + " chats!");

        List<Document> currentBatch;
        FindIterable<Document> iterable = collection.find();
        iterable.batchSize(this.opts.getBatchSize());
        MongoCursor<Document> cursor = iterable.iterator();
        logger.info("Done with that");
        while (!(currentBatch = this.buildBatch(cursor)).isEmpty()) {
            // great! We have a bunch of documents in RAM now :D
            CountDownLatch latch = new CountDownLatch(this.opts.getThreadCap());
            logger.info(String.format("Got a new batch of %d documents", currentBatch.size()));
            if (currentBatch.size() != this.opts.getBatchSize()) {
                logger.info("Last batch!");
            }

            for (int threadNumber = 1; threadNumber <= this.opts.getThreadCap(); threadNumber++) {
                int fromIndex = (threadNumber - 1) * this.itemsPerThread;
                int endIndex = fromIndex + this.itemsPerThread;
                int lastIndexInBatch = currentBatch.size() - 1;
                if (endIndex > lastIndexInBatch) {
                    endIndex = lastIndexInBatch;
                }
                List<Document> forThread = currentBatch.subList(fromIndex, endIndex);
                Thread thread = new Thread(new RunnableProcessor(forThread, this.processor, latch));
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
        List<Document> currentBatch = new ArrayList<>(this.opts.getBatchSize());
        logger.info("Instantiated ArrayList :)");

        while (cursor.hasNext() && currentBatch.size() < this.opts.getBatchSize()) {
            currentBatch.add(cursor.next()); // yield return cursor.next() :(
        }
        return currentBatch;
    }
}

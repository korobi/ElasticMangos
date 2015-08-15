package io.korobi.mongotoelastic.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.korobi.mongotoelastic.logging.InjectLogger;
import io.korobi.mongotoelastic.opt.IOptions;
import io.korobi.mongotoelastic.processor.ChatDocumentProcessor;
import io.korobi.mongotoelastic.processor.IDocumentProcessor;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class MongoChatRetriever {

    @InjectLogger
    private Logger logger;
    private final MongoDatabase database;
    private final IOptions opts;
    private final IDocumentProcessor processor;
    private final int itemsPerThread;
    private final int threadCount;
    private final List<Thread> threadPool;

    @Inject
    public MongoChatRetriever(MongoDatabase database, IOptions opts, ChatDocumentProcessor processor) {
        this.database = database;
        this.opts = opts;
        this.processor = processor;
        this.itemsPerThread = opts.getBatchSize();
        this.threadCount = opts.getThreadCap();
        this.threadPool = new ArrayList<>(opts.getThreadCap());
    }

    public void processData() {
        MongoCollection<Document> collection = this.database.getCollection("chats");
        logger.info("There are " + String.valueOf(collection.count()) + " chats!");

        FindIterable<Document> iterable = collection.find();
        iterable.batchSize(this.opts.getBatchSize());

        MongoCursor<Document> cursor = iterable.iterator();

        for(int i = 0; i < this.threadCount; ++i) {
            Thread thread = new Thread(() -> this.processor.run(cursor, this.itemsPerThread));
            thread.start();
            this.threadPool.add(thread);
        }

        for(Thread thread : this.threadPool) {
            try {
                thread.join();
            } catch(InterruptedException e) {
                logger.error(e);
            }
        }
        cursor.close();
    }

}

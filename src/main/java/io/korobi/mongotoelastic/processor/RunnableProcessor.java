package io.korobi.mongotoelastic.processor;

import org.bson.Document;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class RunnableProcessor implements Runnable {

    private List<Document> documents;
    private IDocumentProcessor processor;
    private CountDownLatch latch;

    public RunnableProcessor(List<Document> documents, IDocumentProcessor processor, CountDownLatch latch) {
        this.documents = documents;
        this.processor = processor;
        this.latch = latch;
    }

    @Override
    public void run() {
        this.processor.run(this.documents);
        this.latch.countDown();
    }
}

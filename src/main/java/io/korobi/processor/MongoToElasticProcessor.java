package io.korobi.processor;

import org.bson.Document;

import java.util.List;
import java.util.logging.Logger;

public class MongoToElasticProcessor implements IDocumentProcessor {

    private Logger logger;

    public MongoToElasticProcessor(Logger logger) {
        this.logger = logger;
    }

    public void run(List<Document> docsToProcess) {
        for (Document doc : docsToProcess) {
            logger.info(doc.toJson());
        }
    }
}

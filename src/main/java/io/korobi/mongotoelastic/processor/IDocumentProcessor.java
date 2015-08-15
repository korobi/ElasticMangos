package io.korobi.mongotoelastic.processor;

import com.mongodb.client.MongoCursor;
import org.bson.Document;

public interface IDocumentProcessor {

    void run(MongoCursor<Document> documents, int bulkSize);
    void run(MongoCursor<Document> documents);
}

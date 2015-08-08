package io.korobi.processor;

import org.bson.Document;

import java.util.List;

@FunctionalInterface
public interface IDocumentProcessor {

    void run(List<Document> docsToProcess);
}

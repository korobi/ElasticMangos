package io.korobi.processor;

import com.google.inject.AbstractModule;

public class ProcessorModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(IDocumentProcessor.class).to(MongoToElasticProcessor.class);
    }
}

package io.korobi.mongotoelastic.processor;

import com.google.inject.AbstractModule;

public class ProcessorModule extends AbstractModule {

    @Override
    protected void configure() {
        this.bind(IDocumentProcessor.class).to(ChatDocumentProcessor.class);
    }
}

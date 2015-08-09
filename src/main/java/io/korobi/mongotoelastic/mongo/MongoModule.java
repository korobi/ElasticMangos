package io.korobi.mongotoelastic.mongo;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.korobi.mongotoelastic.opt.IOptions;

public class MongoModule extends AbstractModule {

    public MongoModule() {
    }

    @Override
    protected void configure() {

    }

    @Provides
    @Singleton
    public MongoClient provideMongoClient(IOptions opts) {
        return new MongoClient(opts.getMongoServerHost(), opts.getMongoServerPort());
    }

    @Provides
    @Singleton
    public MongoDatabase provideMongoDatabase(MongoClient client, IOptions opts) {
        return client.getDatabase(opts.getMongoDatabase());
    }
}

package io.korobi.opts;

public interface IOptions {

    String getMongoServerHost();
    int getMongoServerPort();
    String getMongoDatabase();

    int getBatchSize();
    int getThreadCap();

    String getElasticSearchHost();
    int getElasticSearchPort();
}

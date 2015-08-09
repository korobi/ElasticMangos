package io.korobi.mongotoelastic.opt;

public interface IOptions {

    String getMongoServerHost();
    int getMongoServerPort();
    String getMongoDatabase();

    int getBatchSize();
    int getThreadCap();

    String getElasticSearchHost();
    int getElasticSearchPort();

    boolean getWillReconfigureIndexes();
}

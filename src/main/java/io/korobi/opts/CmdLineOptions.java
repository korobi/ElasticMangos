package io.korobi.opts;

import org.kohsuke.args4j.Option;

public class CmdLineOptions implements IOptions {

    @Option(name = "--mongo-host", usage = "Sets the host of the MongoDB server to connect to.")
    private String mongoHost = "127.0.0.1";

    @Option(name = "--mongo-port", usage = "Sets the port of the MongoDB server to connect to.")
    private int mongoPort = 27017;

    @Option(name = "--mongo-db", usage = "Sets the name of the database to use.", required = true)
    private String mongoDb;

    @Option(name = "--batch-size", usage = "How many documents we grab out of the database at once.")
    private int batchSize = 1000;

    @Option(name = "--thread-cap", usage = "Maximum number of threads we should have running at the same time.")
    private int threadCap = 4;

    public String getMongoServerHost() {
        return mongoHost;
    }

    public int getMongoServerPort() {
        return mongoPort;
    }

    public String getMongoDatabase() {
        return mongoDb;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreadCap() {
        return threadCap;
    }

}

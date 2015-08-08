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

    @Option(name = "--es-host", usage = "ElasticSearch host for transport client.")
    private String esHost = "127.0.0.1";

    @Option(name = "--es-port", usage = "ElasticSearch port for transport client.")
    private int esPort = 9300;

    @Option(name = "--reconfigure-indexes", usage = "Whether to drop and reconfigure all indexes.")
    private boolean reconfigureIndexes = false;

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

    public String getElasticSearchHost() {
        return esHost;
    }

    public int getElasticSearchPort() {
        return esPort;
    }

    public boolean getWillReconfigureIndexes() {
        return reconfigureIndexes;
    }

}

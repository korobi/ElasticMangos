package io.korobi;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mongodb.MongoClient;
import io.korobi.elasticsearch.ElasticSearchModule;
import io.korobi.elasticsearch.IndexInitialiser;
import io.korobi.exceptions.ConnectionException;
import io.korobi.mongo.KeyedChannelBlacklist;
import io.korobi.mongo.MongoModule;
import io.korobi.mongo.MongoRetriever;
import io.korobi.opts.CmdLineOptions;
import io.korobi.opts.IOptions;
import io.korobi.opts.OptionsModule;
import io.korobi.processor.ProcessorModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.util.Scanner;

public class MongoToElastic {

    private Injector injector;

    public MongoToElastic(String[] args) {
        CmdLineOptions opts = handleCommandLineArgs(args);
        if (opts == null) {
            System.exit(-1);
        }

        setupInjector(opts);
        checkConnectedToEs();
        if (opts.getWillReconfigureIndexes()) {
            reconfigureIndexes();
            System.out.println("Hit any key to continue...");
            (new Scanner(System.in)).nextLine();
        }
        injector.getInstance(KeyedChannelBlacklist.class);
        System.exit(-1);

        beginProcessing();
        addShutdownHook();
        cleanupClients();
    }

    private void checkConnectedToEs() {
        TransportClient client = (TransportClient) injector.getInstance(Client.class);
        ImmutableList<DiscoveryNode> nodes = client.connectedNodes();
        if (nodes.isEmpty()) {
            Exception e = new ConnectionException("Verify ES is running!");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void reconfigureIndexes() {
        injector.getInstance(IndexInitialiser.class).initialise();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cleanupClients();
            }
        });
    }

    private void cleanupClients() {
        MongoClient client = injector.getInstance(MongoClient.class);
        client.close();
    }

    private void beginProcessing() {
        injector.getInstance(MongoRetriever.class);
    }

    public CmdLineOptions handleCommandLineArgs(String[] args) {
        CmdLineOptions bean = new CmdLineOptions();
        CmdLineParser parser = new CmdLineParser(bean);
        try {
            parser.parseArgument(args);
            return bean;
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return null;
        }
    }

    private void setupInjector(IOptions opts) {
        this.injector = Guice.createInjector(
                new ElasticSearchModule(), new ProcessorModule(), new MongoModule(), new OptionsModule(opts)
        );
    }

    public static void main(String[] args) {
        new MongoToElastic(args);
    }
}

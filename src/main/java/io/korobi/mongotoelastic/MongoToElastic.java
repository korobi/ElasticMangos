package io.korobi.mongotoelastic;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mongodb.MongoClient;
import io.korobi.mongotoelastic.elasticsearch.ElasticSearchModule;
import io.korobi.mongotoelastic.elasticsearch.IndexInitialiser;
import io.korobi.mongotoelastic.exception.ConnectionException;
import io.korobi.mongotoelastic.logging.LoggingModule;
import io.korobi.mongotoelastic.mongo.KeyedChannelBlacklist;
import io.korobi.mongotoelastic.mongo.MongoModule;
import io.korobi.mongotoelastic.mongo.MongoRetriever;
import io.korobi.mongotoelastic.opt.CmdLineOptions;
import io.korobi.mongotoelastic.opt.IOptions;
import io.korobi.mongotoelastic.opt.OptionsModule;
import io.korobi.mongotoelastic.processor.ProcessorModule;
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
        CmdLineOptions opts = this.handleCommandLineArgs(args);
        if (opts == null) {
            System.exit(-1);
        }

        this.setupInjector(opts);
        this.checkConnectedToEs();
        if (opts.getWillReconfigureIndexes()) {
            this.reconfigureIndexes();
            System.out.println("Hit any key to continue...");
            (new Scanner(System.in)).nextLine();
        }
        this.injector.getInstance(KeyedChannelBlacklist.class);

        this.beginProcessing();
        this.addShutdownHook();
        this.cleanupClients();
    }

    private void checkConnectedToEs() {
        TransportClient client = (TransportClient) this.injector.getInstance(Client.class);
        ImmutableList<DiscoveryNode> nodes = client.connectedNodes();
        if (nodes.isEmpty()) {
            Exception e = new ConnectionException("Verify ES is running!");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void reconfigureIndexes() {
        this.injector.getInstance(IndexInitialiser.class).initialise();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                MongoToElastic.this.cleanupClients();
            }
        });
    }

    private void cleanupClients() {
        MongoClient client = this.injector.getInstance(MongoClient.class);
        client.close();
    }

    private void beginProcessing() {
        MongoRetriever r = this.injector.getInstance(MongoRetriever.class);
        r.processData();
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
            new ElasticSearchModule(), new ProcessorModule(), new MongoModule(), new OptionsModule(opts),
            new LoggingModule()
        );
    }

    public static void main(String[] args) {
        new MongoToElastic(args);
    }
}

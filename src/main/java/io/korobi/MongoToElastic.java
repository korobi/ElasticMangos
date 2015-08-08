package io.korobi;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mongodb.MongoClient;
import io.korobi.mongo.MongoModule;
import io.korobi.mongo.MongoRetriever;
import io.korobi.opts.CmdLineOptions;
import io.korobi.opts.IOptions;
import io.korobi.opts.OptionsModule;
import io.korobi.processor.ProcessorModule;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class MongoToElastic {

    private Injector injector;

    public MongoToElastic(String[] args) {
        CmdLineOptions opts = handleCommandLineArgs(args);
        if (opts == null) {
            System.exit(-1);
        }

        setupInjector(opts);
        beginProcessing();
        addShutdownHook();
        cleanupClients();
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

    private CmdLineOptions handleCommandLineArgs(String[] args) {
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
        this.injector = Guice.createInjector(new ProcessorModule(), new MongoModule(), new OptionsModule(opts));
    }

    public static void main(String[] args) {
        new MongoToElastic(args);
    }
}

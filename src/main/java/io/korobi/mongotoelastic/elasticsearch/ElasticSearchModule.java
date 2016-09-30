package io.korobi.mongotoelastic.elasticsearch;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.korobi.mongotoelastic.opt.IOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchModule extends AbstractModule {

    @Singleton
    @Provides
    public Client provideClient(IOptions opts) {
        // http://elasticsearch-users.115913.n3.nabble.com/Is-NodeClient-thread-safe-td2816264.html

        TransportClient client = TransportClient.builder().build();
        try {
            client = client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(opts.getElasticSearchHost()), opts.getElasticSearchPort()));
        } catch (UnknownHostException e) {
            System.err.printf("Unknown host: %s, check that configured value is sensible%n", opts
                    .getElasticSearchHost());
            e.printStackTrace();
            System.exit(-1);
        }
        return client;
    }

    @Override
    protected void configure() {

    }
}

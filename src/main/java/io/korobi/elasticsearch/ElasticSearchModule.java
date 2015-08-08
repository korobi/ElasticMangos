package io.korobi.elasticsearch;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.korobi.opts.IOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import javax.inject.Singleton;

public class ElasticSearchModule extends AbstractModule {

    @Singleton
    @Provides
    public Client provideClient(IOptions opts) {
        // http://elasticsearch-users.115913.n3.nabble.com/Is-NodeClient-thread-safe-td2816264.html
        return new TransportClient()
        .addTransportAddress(new InetSocketTransportAddress(opts.getElasticSearchHost(), opts.getElasticSearchPort()));
    }

    @Override
    protected void configure() {

    }
}

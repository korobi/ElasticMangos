package io.korobi.mongotoelastic.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class KeyedChannelBlacklist implements IChannelBlacklist {

    private MongoDatabase database;
    private Map<String, Set<String>> cachedChannels;

    @Inject
    public KeyedChannelBlacklist(MongoDatabase database) {
        this.database = database;
        this.createCache();
    }

    private void createCache() {
        MongoCollection<Document> channels = this.database.getCollection("channels");
        FindIterable<Document> allChannels = channels.find();
        Stream<Document> stream = StreamSupport.stream(allChannels.spliterator(), false);
        this.cachedChannels = stream
            .filter(c -> c.containsKey("key") && c.get("key") != null)
            .collect(
                Collectors.groupingBy(
                    channel -> channel.getString("network"),
                    HashMap::new,
                    Collector.<Document, Set<String>, Set<String>>of(
                        HashSet::new, (s, d) -> s.add(d.getString("channel")),
                        (left, right) -> {
                            left.addAll(right);
                            return left;
                        },
                        Collections::unmodifiableSet
                    )
                )
            );
    }

    @Override
    public boolean isBlacklisted(String network, String channel) {
        return this.cachedChannels.getOrDefault(network, Collections.emptySet()).contains(channel);
    }
}

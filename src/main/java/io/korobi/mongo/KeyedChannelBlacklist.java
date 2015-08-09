package io.korobi.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KeyedChannelBlacklist implements IChannelBlacklist {

    private MongoDatabase database;
    private Map<String, Set<String>> cachedChannels;

    @Inject
    public KeyedChannelBlacklist(MongoDatabase database) {
        this.database = database;
        createCache();
    }

    private void createCache() {
        MongoCollection<Document> channels = database.getCollection("channels");
        FindIterable<Document> allChannels = channels.find();
        Stream<Document> stream = StreamSupport.stream(allChannels.spliterator(), false);
        cachedChannels = stream
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

    public boolean isBlacklisted(String channel, String network) {
        return cachedChannels.getOrDefault(network, Collections.emptySet()).contains(channel);
    }
}

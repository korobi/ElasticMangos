package io.korobi.mongotoelastic.mongo;

import javax.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
/**
 * In-memory map of channel object IDs so we don't have to hit Mongo
 * for every chat in order to look them up.
 *
 * Note this instance will be shared amongst many threads and should
 * be thread-safe.
 */
public class ChannelIdMemoryCache implements IChannelIdLookup {

    public static final int INITIAL_NETWORK_CAPACITY = 10;
    public static final int INITIAL_CHANNEL_CAPACITY = 50;

    private final Map<String, Map<String, String>> cache = new ConcurrentHashMap<>(INITIAL_NETWORK_CAPACITY);

    @Override
    public Optional<String> getChannelObjectId(String network, String channel) {
        return Optional.empty();
    }

    @Override
    public void provideChannelId(String objectId, String network, String channel) {
        if (cache.containsKey(network)) {
            cache.get(network).put(channel, objectId);
        } else {
            // I miss my collection initializers :(
            /*
               var dict = new Dictionary<string, string>() {
                   { channel, objectId }
               };
            */
            ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>(INITIAL_CHANNEL_CAPACITY);
            map.put(channel, objectId);
            cache.put(network, map);
        }
    }
}

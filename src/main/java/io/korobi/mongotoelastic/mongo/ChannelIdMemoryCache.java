package io.korobi.mongotoelastic.mongo;

import javax.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class ChannelIdMemoryCache implements IChannelIdLookup {

    public static final int NETWORK_CAPACITY = 10;
    public static final int CHANNEL_CAPACITY = 40;

    private final Map<String, Map<String, String>> cache = new ConcurrentHashMap<>(NETWORK_CAPACITY);

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
            ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>(CHANNEL_CAPACITY);
            map.put(channel, objectId);
            cache.put(network, map);
        }
    }
}

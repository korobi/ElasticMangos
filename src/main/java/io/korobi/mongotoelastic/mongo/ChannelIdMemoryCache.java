package io.korobi.mongotoelastic.mongo;

import javax.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class ChannelIdMemoryCache implements IChannelIdLookup {

    private Map<String, Map<String, String>> cache = new ConcurrentHashMap<>(10);

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
            ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>(40);
            map.put(channel, objectId);
            cache.put(network, map);
        }
    }
}

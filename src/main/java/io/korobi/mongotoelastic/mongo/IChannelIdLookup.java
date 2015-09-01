package io.korobi.mongotoelastic.mongo;

import java.util.Optional;

@FunctionalInterface
public interface IChannelIdLookup {

    /**
     * Looks up a channel object ID from some source.
     * @param network The slug for the network as retrieved from the database.
     * @param channel The name of the channel (including # or equivalent prefix character).
     * @return The channel object ID hex string (if available).
     */
    Optional<String> getChannelObjectId(String network, String channel);
}

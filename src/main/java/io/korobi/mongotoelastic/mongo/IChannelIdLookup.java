package io.korobi.mongotoelastic.mongo;

import java.util.Optional;

public interface IChannelIdLookup {

    /**
     * Looks up a channel object ID from some source.
     * @param network The slug for the network as retrieved from the database.
     * @param channel The name of the channel (including # or equivalent prefix character).
     * @return The channel object ID hex string (if available).
     */
    Optional<String> getChannelObjectId(String network, String channel);

    /**
     * Provides a channel object id for a network-channel combination to be added to the store.
     * @param objectId The hexadecimal representation of the object ID.
     * @param network The slug for the network as retrieved from the database.
     * @param channel The name of the channel (including # or equivalent prefix character).
     */
    void provideChannelId(String objectId, String network, String channel);
}

package io.korobi.mongotoelastic.mongo;

@FunctionalInterface
public interface IChannelBlacklist {

    /**
     * Determine if a channel is blacklisted from being indexed.
     * @param network The slug for the network as retrieved from the database.
     * @param channel The name of the channel (including # or equivalent prefix character)
     * @return Whether or not the channel should be blacklisted from the index.
     */
    boolean isBlacklisted(String network, String channel);
}

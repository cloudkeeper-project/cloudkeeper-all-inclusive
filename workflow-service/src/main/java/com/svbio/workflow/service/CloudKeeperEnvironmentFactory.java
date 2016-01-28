package com.svbio.workflow.service;

import com.svbio.cloudkeeper.model.api.CloudKeeperEnvironment;

/**
 * Factory of {@link CloudKeeperEnvironment} instances.
 */
@FunctionalInterface
interface CloudKeeperEnvironmentFactory {
    /**
     * Creates and returns a {@link CloudKeeperEnvironment}.
     *
     * @param prefix prefix for the staging area, the meaning of prefix depends on the configured staging-area service
     * @param cleaningRequested whether intermediate results should be removed from the staging area as soon as they are
     *     no longer needed
     * @return the new {@link CloudKeeperEnvironment}
     */
    CloudKeeperEnvironment create(String prefix, boolean cleaningRequested);
}

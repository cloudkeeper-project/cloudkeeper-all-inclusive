package com.svbio.workflow.service;

import com.svbio.cloudkeeper.model.api.staging.StagingAreaProvider;

/**
 * Subservice that provides CloudKeeper staging areas.
 */
@FunctionalInterface
interface StagingAreaService {
    /**
     * Returns a CloudKeeper {@link StagingAreaProvider} for the given prefix.
     *
     * @param prefix prefix that should be used to construct the initial staging area
     * @return the CloudKeeper {@link StagingAreaProvider} instance
     * @throws NullPointerException if the argument is null
     */
    StagingAreaProvider provideInitialStagingAreaProvider(String prefix);
}

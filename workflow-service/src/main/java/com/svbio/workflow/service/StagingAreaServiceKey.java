package com.svbio.workflow.service;

import dagger.MapKey;

/**
 * Annotation for provider methods that yield an entry in the {@link StagingAreaServiceQualifier}-annotated map of
 * all staging-area services.
 */
@MapKey
@interface StagingAreaServiceKey {
    String value();
}

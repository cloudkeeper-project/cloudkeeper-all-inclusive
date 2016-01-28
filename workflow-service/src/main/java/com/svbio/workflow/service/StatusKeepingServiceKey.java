package com.svbio.workflow.service;

import dagger.MapKey;

/**
 * Annotation for provider methods that yield an entry in the {@link StatusKeepingServiceQualifier}-annotated map of
 * all status-keeping services.
 */
@MapKey
@interface StatusKeepingServiceKey {
    String value();
}

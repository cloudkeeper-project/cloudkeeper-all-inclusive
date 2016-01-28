package com.svbio.workflow.runtimecontext;

import dagger.MapKey;

/**
 * Annotation for provider methods that yield an entry in the {@link RuntimeContextFactoryQualifier}-annotated map of
 * all runtime-context factories.
 */
@MapKey
@interface RuntimeContextFactoryKey {
    String value();
}

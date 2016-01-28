package com.svbio.workflow.service;

import dagger.MapKey;

/**
 * Annotation for provider methods that yield an entry in the {@link SimpleModuleExecutorQualifier}-annotated map of
 * all simple-module executors.
 */
@MapKey
@interface SimpleModuleExecutorKey {
    String value();
}

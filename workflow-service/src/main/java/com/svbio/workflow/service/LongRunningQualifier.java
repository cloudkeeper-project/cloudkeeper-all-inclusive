package com.svbio.workflow.service;

import javax.inject.Qualifier;

/**
 * Qualifier for thread pools accepting long-running tasks.
 */
@Qualifier
@interface LongRunningQualifier { }

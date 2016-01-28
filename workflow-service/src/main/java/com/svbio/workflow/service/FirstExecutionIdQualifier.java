package com.svbio.workflow.service;

import javax.inject.Qualifier;

/**
 * Qualifier for {@code long} value that is the first execution id used by the CloudKeeper master interpreter.
 */
@Qualifier
@interface FirstExecutionIdQualifier { }

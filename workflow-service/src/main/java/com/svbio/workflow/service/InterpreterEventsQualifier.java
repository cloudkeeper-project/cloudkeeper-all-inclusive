package com.svbio.workflow.service;

import javax.inject.Qualifier;

/**
 * Qualifier for the set of {@link com.svbio.cloudkeeper.interpreter.EventSubscription} instances that will be passed to
 * {@link com.svbio.cloudkeeper.interpreter.CloudKeeperEnvironmentBuilder#setEventListeners(java.util.List)}.
 */
@Qualifier
@interface InterpreterEventsQualifier { }

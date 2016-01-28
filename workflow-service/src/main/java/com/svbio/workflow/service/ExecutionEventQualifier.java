package com.svbio.workflow.service;

import javax.inject.Qualifier;

/**
 * Qualifier for the set of {@link akka.actor.ActorRef} instances that will be subscribed to events of type
 * {@link ExecutionEvent}.
 */
@Qualifier
@interface ExecutionEventQualifier { }

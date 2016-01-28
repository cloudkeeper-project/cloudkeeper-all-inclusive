package com.svbio.workflow.servlet;

import javax.inject.Scope;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Scope annotation (for Dagger components and provider methods).
 *
 * <p>This annotations guarantees that only a single instance will be created within this scope.
 *
 * @see <a href="http://goo.gl/mW474Z">Dagger Design Doc</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Scope
@interface WorkflowServletScope { }

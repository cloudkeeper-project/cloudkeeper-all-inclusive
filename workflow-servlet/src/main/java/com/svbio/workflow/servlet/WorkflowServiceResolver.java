package com.svbio.workflow.servlet;

import com.svbio.workflow.api.WorkflowService;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.ext.ContextResolver;
import java.util.Objects;

/**
 * JAX-RS provider (see JAX-RS 2.0, §4) that provides {@link WorkflowService}.
 *
 * <p>This provider is instantiated outside of the JAX-RS runtime. It also does not need to have a public
 * constructor (§4.1.2).
 */
final class WorkflowServiceResolver implements ContextResolver<WorkflowService> {
    private final WorkflowService service;

    @Inject
    WorkflowServiceResolver(WorkflowService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public WorkflowService getContext(@Nullable Class<?> type) {
        return service;
    }
}

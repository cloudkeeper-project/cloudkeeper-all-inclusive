package com.svbio.workflow.service;

import com.svbio.workflow.api.WorkflowService;
import com.svbio.workflow.runtimecontext.RuntimeContextComponent;
import dagger.Component;

/**
 * Dagger component that provides a {@link WorkflowService}.
 */
@Component(
    dependencies = RuntimeContextComponent.class,
    modules = {
        WorkflowServiceModule.class,
        DatabaseLoggingModule.class
    }
)
@WorkflowServiceScope
public interface WorkflowServiceComponent {
    /**
     * Returns a {@link WorkflowService} for managing workflow executions.
     *
     * @return the {@link WorkflowService}
     */
    WorkflowService getWorkflowService();
}

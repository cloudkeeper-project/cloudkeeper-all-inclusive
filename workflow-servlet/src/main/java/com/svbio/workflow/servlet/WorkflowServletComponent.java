package com.svbio.workflow.servlet;

import com.svbio.workflow.service.WorkflowServiceComponent;
import dagger.Component;

import javax.servlet.http.HttpServlet;

/**
 * Dagger component that provides a {@link javax.servlet.http.HttpServlet}.
 */
@Component(
    dependencies = WorkflowServiceComponent.class,
    modules = WorkflowServletModule.class
)
@WorkflowServletScope
public interface WorkflowServletComponent {
    /**
     * Returns the {@link HttpServlet} that exposes the configured {@link com.svbio.workflow.api.WorkflowService} via
     * a RESTful interface.
     *
     * @return the {@link HttpServlet}
     */
    HttpServlet getServlet();
}

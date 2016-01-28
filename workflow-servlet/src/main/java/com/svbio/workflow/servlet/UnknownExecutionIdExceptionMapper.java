package com.svbio.workflow.servlet;

import com.svbio.workflow.api.UnknownExecutionIdException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * JAX-RS exception mapper for expected exceptions.
 */
final class UnknownExecutionIdExceptionMapper implements ExceptionMapper<UnknownExecutionIdException> {
    @Override
    public Response toResponse(UnknownExecutionIdException exception) {
        return Response.status(Response.Status.NOT_FOUND).build();
    }
}

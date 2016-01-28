package com.svbio.workflow.api;

/**
 * Signals that an execution ID is not known, either because it never existed or because the execution has terminated.
 */
public class UnknownExecutionIdException extends Exception {
    private static final long serialVersionUID = 7661400964421735876L;

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param executionId execution ID that is not known
     */
    public UnknownExecutionIdException(long executionId) {
        super(String.format("Unknown execution ID %d.", executionId));
    }
}

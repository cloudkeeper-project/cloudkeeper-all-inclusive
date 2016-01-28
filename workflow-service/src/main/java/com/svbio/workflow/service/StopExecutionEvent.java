package com.svbio.workflow.service;

/**
 * Event to indicate that the execution of a workflow has ended.
 *
 * <p>A response (which can be an arbitrary {@link Object}) is expected to be sent once the receiver of this message
 * has entirely finished handling the execution id represented by this message.
 */
final class StopExecutionEvent extends ExecutionEvent {
    StopExecutionEvent(long executionId) {
        super(executionId);
    }
}

package com.svbio.workflow.service;

import java.util.Objects;

/**
 * A {@link Runnable} task, together with submission information.
 */
final class Task {
    private final long submissionTimeMillis;
    private final StackTraceElement[] stackTrace;
    private final Runnable runnable;

    Task(long submissionTimeMillis, StackTraceElement[] stackTrace, Runnable runnable) {
        Objects.requireNonNull(submissionTimeMillis);
        Objects.requireNonNull(stackTrace);
        Objects.requireNonNull(runnable);
        this.submissionTimeMillis = submissionTimeMillis;
        this.stackTrace = stackTrace;
        this.runnable = runnable;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(256);
        builder.append("Task submitted at time ").append(submissionTimeMillis);
        if (stackTrace.length > 0) {
            for (StackTraceElement element: stackTrace) {
                builder.append("\n\tat ").append(element);
            }
        }
        return builder.toString();
    }

    void run() {
        runnable.run();
    }
}

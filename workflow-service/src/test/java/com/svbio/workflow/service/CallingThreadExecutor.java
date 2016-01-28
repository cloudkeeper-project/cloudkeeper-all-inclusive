package com.svbio.workflow.service;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

final class CallingThreadExecutor implements Executor {
    private final ConcurrentLinkedQueue<Task> pendingTasks = new ConcurrentLinkedQueue<>();

    @Override
    public void execute(Runnable command) {
        pendingTasks.add(new Task(System.currentTimeMillis(), Thread.currentThread().getStackTrace(), command));
    }

    boolean hasPendingTasks() {
        return !pendingTasks.isEmpty();
    }

    /**
     * Runs the next task, in the current thread.
     *
     * @throws java.util.NoSuchElementException if there are no more pending tasks
     */
    void executeNext() {
        Task task = pendingTasks.remove();
        task.run();
    }

    /**
     * Runs all pending tasks in the current thread, until the queue of pending tasks is empty.
     */
    void executeAll() {
        while (hasPendingTasks()) {
            executeNext();
        }
    }
}

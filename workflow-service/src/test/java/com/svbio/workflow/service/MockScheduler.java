package com.svbio.workflow.service;

import akka.actor.AbstractScheduler;
import akka.actor.Cancellable;
import akka.event.LoggingAdapter;
import com.typesafe.config.Config;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Mock Akka scheduler that also serves as milliseconds-based clock.
 */
public final class MockScheduler extends AbstractScheduler implements Clock {
    private static final double MAX_FREQUENCY = 1000;

    private long currentTime = 0;
    private final SortedMap<Long, List<ScheduledTask>> pendingTasks
        = Collections.synchronizedSortedMap(new TreeMap<>());
    private final Object monitor = new Object();

    public MockScheduler(Config config, LoggingAdapter loggingAdapter, ThreadFactory threadFactory) { }

    private abstract class ScheduledTask implements Cancellable {
        private final Task task;
        private volatile long scheduledTime = -1;

        private ScheduledTask(Task task) {
            this.task = task;
        }

        @Override
        public final boolean cancel() {
            synchronized (monitor) {
                @Nullable List<ScheduledTask> scheduledTasks = pendingTasks.get(scheduledTime);
                if (scheduledTasks != null) {
                    boolean couldRemove = scheduledTasks.remove(this);
                    assert couldRemove;
                    if (scheduledTasks.isEmpty()) {
                        pendingTasks.remove(scheduledTime);
                    }
                    scheduledTime = -1;
                    return true;
                }
                return false;
            }
        }

        @Override
        public boolean isCancelled() {
            return scheduledTime < 0;
        }
    }

    private final class OneOffScheduledTask extends ScheduledTask {
        private OneOffScheduledTask(Task task) {
            super(task);
        }
    }

    private final class RepeatedlyScheduledTask extends ScheduledTask {
        private final long intervalMillis;

        private RepeatedlyScheduledTask(Task task, long intervalMillis) {
            super(task);
            this.intervalMillis = intervalMillis;
        }
    }

    /**
     * Advances the current time by the given time delta and runs all tasks that were scheduled <em>before</em> the new
     * current time.
     *
     * @param delta time delta
     */
    void advanceTime(long delta) {
        synchronized (monitor) {
            currentTime += delta;

            SortedMap<Long, List<ScheduledTask>> headMap = pendingTasks.headMap(currentTime);
            List<RepeatedlyScheduledTask> repeatedlyScheduledTasks = new ArrayList<>();
            for (List<ScheduledTask> scheduledTasks: headMap.values()) {
                for (ScheduledTask scheduledTask: scheduledTasks) {
                    scheduledTask.task.run();
                    if (scheduledTask instanceof RepeatedlyScheduledTask) {
                        repeatedlyScheduledTasks.add((RepeatedlyScheduledTask) scheduledTask);
                    }
                }
            }
            headMap.clear();
            for (RepeatedlyScheduledTask repeatedlyScheduledTask: repeatedlyScheduledTasks) {
                addScheduledTask(repeatedlyScheduledTask, repeatedlyScheduledTask.intervalMillis);
            }
        }
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    public long getCurrentTime() {
        synchronized (monitor) {
            return currentTime;
        }
    }

    private ScheduledTask addScheduledTask(ScheduledTask scheduledTask, long initialDelayMillis) {
        synchronized (monitor) {
            long scheduledTime = currentTime + initialDelayMillis;
            scheduledTask.scheduledTime = scheduledTime;
            @Nullable List<ScheduledTask> scheduledTasks = pendingTasks.get(scheduledTime);
            if (scheduledTasks == null) {
                scheduledTasks = new ArrayList<>();
                pendingTasks.put(currentTime + initialDelayMillis, scheduledTasks);
            }
            scheduledTasks.add(scheduledTask);
        }
        return scheduledTask;
    }

    @Override
    public Cancellable schedule(FiniteDuration initialDelay, FiniteDuration interval, Runnable runnable,
            ExecutionContext executor) {
        RepeatedlyScheduledTask scheduledTask = new RepeatedlyScheduledTask(
            new Task(System.currentTimeMillis(), Thread.currentThread().getStackTrace(), runnable),
            interval.toMillis()
        );
        addScheduledTask(scheduledTask, initialDelay.toMillis());
        return scheduledTask;
    }

    @Override
    public Cancellable scheduleOnce(FiniteDuration delay, Runnable runnable, ExecutionContext executor) {
        OneOffScheduledTask scheduledTask = new OneOffScheduledTask(
            new Task(System.currentTimeMillis(), Thread.currentThread().getStackTrace(), runnable)
        );
        addScheduledTask(scheduledTask, delay.toMillis());
        return scheduledTask;
    }

    @Override
    public double maxFrequency() {
        return MAX_FREQUENCY;
    }
}

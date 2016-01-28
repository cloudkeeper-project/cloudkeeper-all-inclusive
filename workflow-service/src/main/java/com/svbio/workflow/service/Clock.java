package com.svbio.workflow.service;

import java.util.concurrent.TimeUnit;

/**
 * Clock interface that provides timestamps.
 */
interface Clock {
    /**
     * Returns the time unit of this clock.
     */
    TimeUnit getTimeUnit();

    /**
     * Returns the current time.
     */
    long getCurrentTime();
}

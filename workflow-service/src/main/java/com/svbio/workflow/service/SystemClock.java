package com.svbio.workflow.service;

import java.util.concurrent.TimeUnit;

/**
 * Clock based on the system time.
 */
enum SystemClock implements Clock {
    NANO {
        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.NANOSECONDS;
        }

        @Override
        public long getCurrentTime()  {
            return System.nanoTime();
        }
    };
}

package com.svbio.workflow.util;

import org.eclipse.persistence.logging.SessionLog;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SLF4JSessionLogTest {
    @Test
    public void testShouldLog() {
        SLF4JSessionLog logger
            = new SLF4JSessionLog(new MockLoggerFactory(SLF4JSessionLog.Level.INFO, new ArrayList<>()));
        logger.setLevel(SessionLog.ALL);
        Assert.assertFalse(logger.shouldLog(SessionLog.FINEST, SessionLog.SQL));
        Assert.assertFalse(logger.shouldLog(SessionLog.FINER, SessionLog.SQL));
        Assert.assertFalse(logger.shouldLog(SessionLog.FINE, SessionLog.SQL));
        Assert.assertFalse(logger.shouldLog(SessionLog.CONFIG, SessionLog.SQL));
        Assert.assertTrue(logger.shouldLog(SessionLog.INFO, SessionLog.SQL));
        Assert.assertTrue(logger.shouldLog(SessionLog.WARNING, SessionLog.SQL));
        Assert.assertTrue(logger.shouldLog(SessionLog.SEVERE, SessionLog.SQL));
        Assert.assertFalse(logger.shouldLog(SessionLog.OFF, SessionLog.SQL));

        logger.setLevel(SessionLog.WARNING);
        Assert.assertFalse(logger.shouldLog(SessionLog.CONFIG, SessionLog.SQL));
        Assert.assertFalse(logger.shouldLog(SessionLog.INFO, SessionLog.SQL));
        Assert.assertTrue(logger.shouldLog(SessionLog.WARNING, SessionLog.SQL));
    }

    @Test
    public void testLog() {
        IOException exception = new IOException("Artificial exception just for testing!");

        List<LogEntry> logEntries = new ArrayList<>();
        SLF4JSessionLog logger = new SLF4JSessionLog(new MockLoggerFactory(SLF4JSessionLog.Level.TRACE, logEntries));
        logger.setLevel(SessionLog.ALL);
        logger.log(SessionLog.FINE, SessionLog.SQL, "Hello {0}", new Object[] { "World" }, false);
        logger.log(SessionLog.WARNING, null, "Foo", null, false);
        logger.logThrowable(SessionLog.SEVERE, SessionLog.CONNECTION, exception);
        Assert.assertEquals(
            logEntries,
            Arrays.asList(
                new LogEntry(SLF4JSessionLog.Level.DEBUG, SLF4JSessionLog.ECLIPSELINK_NAMESPACE + '.' + SessionLog.SQL,
                    "Hello World"),
                new LogEntry(SLF4JSessionLog.Level.WARN, SLF4JSessionLog.ECLIPSELINK_NAMESPACE, "Foo"),
                new FailureLogEntry(SLF4JSessionLog.Level.ERROR,
                    SLF4JSessionLog.ECLIPSELINK_NAMESPACE + '.' + SessionLog.CONNECTION, "", exception)
            )
        );
    }

    private static class LogEntry {
        private final SLF4JSessionLog.Level level;
        private final String loggerName;
        private final String message;

        private LogEntry(SLF4JSessionLog.Level level, String loggerName, String message) {
            this.level = level;
            this.loggerName = loggerName;
            this.message = message;
        }

        @Override
        public boolean equals(@Nullable Object otherObject) {
            if (this == otherObject) {
                return true;
            } else if (otherObject == null || getClass() != otherObject.getClass()) {
                return false;
            }

            LogEntry other = (LogEntry) otherObject;
            return level == other.level
                && loggerName.equals(other.loggerName)
                && message.equals(other.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, loggerName, message);
        }
    }

    private static final class FailureLogEntry extends LogEntry {
        private final Throwable throwable;

        private FailureLogEntry(SLF4JSessionLog.Level level, String loggerName, String message, Throwable throwable) {
            super(level, loggerName, message);
            this.throwable = throwable;
        }

        @Override
        public boolean equals(@Nullable Object otherObject) {
            return this == otherObject || (
                super.equals(otherObject)
                && throwable.equals(((FailureLogEntry) otherObject).throwable)
            );
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + throwable.hashCode();
        }
    }

    private static final class MockLoggerFactory implements ILoggerFactory {
        private final SLF4JSessionLog.Level level;
        private final List<LogEntry> logEntries;
        private final Map<String, MockLogger> loggers = new LinkedHashMap<>();

        private MockLoggerFactory(SLF4JSessionLog.Level level, List<LogEntry> logEntries) {
            this.level = level;
            this.logEntries = logEntries;
        }

        @Override
        public Logger getLogger(String name) {
            @Nullable MockLogger logger = loggers.get(name);
            if (logger == null) {
                logger = new MockLogger(level, name, logEntries);
                loggers.put(name, logger);
            }
            return logger;
        }
    }

    private static final class MockLogger extends MarkerIgnoringBase {
        private final SLF4JSessionLog.Level level;
        private final String loggerName;
        private final List<LogEntry> logEntries;

        private MockLogger(SLF4JSessionLog.Level level, String loggerName, List<LogEntry> logEntries) {
            this.level = level;
            this.loggerName = loggerName;
            this.logEntries = logEntries;
        }

        private Object writeReplace() {
            throw new UnsupportedOperationException("Serialization not supported!");
        }

        @Override
        public String getName() {
            return loggerName;
        }

        @Override
        public boolean isTraceEnabled() {
            return SLF4JSessionLog.Level.TRACE.compareTo(level) >= 0;
        }

        @Override
        public void trace(String msg) {
            if (!isTraceEnabled()) {
                return;
            }
            logEntries.add(new LogEntry(SLF4JSessionLog.Level.TRACE, loggerName, msg));
        }

        @Override
        public void trace(String msg, Object object) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void trace(String msg, Object object1, Object object2) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void trace(String msg, Object... objects) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void trace(String msg, Throwable throwable) {
            logEntries.add(new FailureLogEntry(SLF4JSessionLog.Level.TRACE, loggerName, msg, throwable));
        }


        @Override
        public boolean isDebugEnabled() {
            return SLF4JSessionLog.Level.DEBUG.compareTo(level) >= 0;
        }

        @Override
        public void debug(String msg) {
            logEntries.add(new LogEntry(SLF4JSessionLog.Level.DEBUG, loggerName, msg));
        }

        @Override
        public void debug(String msg, Object object) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void debug(String msg, Object object1, Object object2) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void debug(String msg, Object... objects) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void debug(String msg, Throwable throwable) {
            logEntries.add(new FailureLogEntry(SLF4JSessionLog.Level.DEBUG, loggerName, msg, throwable));
        }

        @Override
        public boolean isInfoEnabled() {
            return SLF4JSessionLog.Level.INFO.compareTo(level) >= 0;
        }

        @Override
        public void info(String msg) {
            logEntries.add(new LogEntry(SLF4JSessionLog.Level.INFO, loggerName, msg));
        }

        @Override
        public void info(String msg, Object object) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void info(String msg, Object object1, Object object2) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void info(String msg, Object... objects) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void info(String msg, Throwable throwable) {
            logEntries.add(new FailureLogEntry(SLF4JSessionLog.Level.INFO, loggerName, msg, throwable));
        }

        @Override
        public boolean isWarnEnabled() {
            return SLF4JSessionLog.Level.WARN.compareTo(level) >= 0;
        }

        @Override
        public void warn(String msg) {
            logEntries.add(new LogEntry(SLF4JSessionLog.Level.WARN, loggerName, msg));
        }

        @Override
        public void warn(String msg, Object object) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void warn(String msg, Object... objects) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void warn(String msg, Object object1, Object object2) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void warn(String msg, Throwable throwable) {
            logEntries.add(new FailureLogEntry(SLF4JSessionLog.Level.WARN, loggerName, msg, throwable));
        }

        @Override
        public boolean isErrorEnabled() {
            return SLF4JSessionLog.Level.ERROR.compareTo(level) >= 0;
        }

        @Override
        public void error(String msg) {
            logEntries.add(new LogEntry(SLF4JSessionLog.Level.ERROR, loggerName, msg));
        }

        @Override
        public void error(String msg, Object object) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void error(String msg, Object object1, Object object2) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void error(String msg, Object... objects) {
            Assert.fail("not expected to be called");
        }

        @Override
        public void error(String msg, Throwable throwable) {
            logEntries.add(new FailureLogEntry(SLF4JSessionLog.Level.ERROR, loggerName, msg, throwable));
        }
    }
}

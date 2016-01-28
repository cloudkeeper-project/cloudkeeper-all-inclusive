package com.svbio.workflow.util;

import org.eclipse.persistence.logging.AbstractSessionLog;
import org.eclipse.persistence.logging.SessionLog;
import org.eclipse.persistence.logging.SessionLogEntry;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link SessionLog} that uses Slf4j as EclipseLink logging service.
 *
 * <p>EclipseLink has finer-grained log levels than Slf4j, and therefore log levels are mapped as follows:
 * <ul>
 *   <li>{@link SessionLog#FINEST} and {@link SessionLog#FINER} are mapped to 'trace'</li>
 *   <li>{@link SessionLog#FINE} and {@link SessionLog#CONFIG} are mapped to 'debug'</li>
 *   </li>{@link SessionLog#INFO} is mapped to 'info'</li>
 *   <li>{@link SessionLog#WARNING} is mapped to 'warn'</li>
 *   <li>{@link SessionLog#SEVERE} is mapped to 'error'</li>
 * </ul>
 *
 * <p>To enable this class as a logger for EclipseLink, configure EclipseLink persistence unit property
 * {@link org.eclipse.persistence.config.PersistenceUnitProperties#LOGGING_LOGGER} as the name of this class. For
 * instance, {@code properties.put(PersistenceUnitProperties.LOGGING_LOGGER, SLF4JSessionLog.class.getName())}.
 *
 * <p>Note that logging is actually controlled by two level settings when using this class: The EclipseLink log level,
 * available as {@link #getLevel()} and typically controlled by property
 * {@link org.eclipse.persistence.config.PersistenceUnitProperties#LOGGING_LEVEL}, and by the Slf4j log level. For
 * instance, when using the slf4j-simple backend, then the Slf4j log level is determined by system property
 * {@code org.slf4j.simpleLogger.defaultLogLevel} (see
 * <a href="http://www.slf4j.org/api/org/slf4j/impl/SimpleLogger.html">JavaDoc of class SimpleLogger</a>). Typically,
 * one would configure property {@link org.eclipse.persistence.config.PersistenceUnitProperties#LOGGING_LEVEL} to
 * {@link SessionLog#ALL_LABEL}, and control logging only through Slf4j.
 */
public final class SLF4JSessionLog extends AbstractSessionLog {
    static final String ECLIPSELINK_NAMESPACE = "org.eclipse.persistence";

    private final Logger defaultLogger;
    private final Map<String, Logger> loggers;

    /**
     * Constructs a new session log instance that uses the Slf4j logger factory returned by
     * {@link LoggerFactory#getILoggerFactory()}.
     *
     * @see #SLF4JSessionLog(ILoggerFactory)
     */
    public SLF4JSessionLog() {
        this(LoggerFactory.getILoggerFactory());
    }

    /**
     * Constructs a new session log instance that uses the given Slf4j logger factory.
     *
     * <p>One {@link Logger} instances is created for each element of {@link SessionLog#loggerCatagories}, plus one
     * default logger.
     *
     * @param loggerFactory logger factory used for creating {@link Logger} instances
     */
    public SLF4JSessionLog(ILoggerFactory loggerFactory) {
        Objects.requireNonNull(loggerFactory);
        defaultLogger = loggerFactory.getLogger(ECLIPSELINK_NAMESPACE);
        HashMap<String, Logger> map = new HashMap<>(1 + SessionLog.loggerCatagories.length);
        map.put(ECLIPSELINK_NAMESPACE, defaultLogger);
        for (String loggerCategory: SessionLog.loggerCatagories) {
            map.put(loggerCategory, loggerFactory.getLogger(ECLIPSELINK_NAMESPACE + '.' + loggerCategory));
        }
        loggers = Collections.unmodifiableMap(map);
    }

    @Override
    public void log(SessionLogEntry logEntry) {
        Logger logger = getLogger(logEntry.getNameSpace());
        Level sl4fjLevel = Level.fromEclipse(logEntry.getLevel());
        String message = formatMessage(logEntry);
        if (logEntry.hasException()) {
            sl4fjLevel.logThrowable(logger, message, logEntry.getException());
        } else {
            sl4fjLevel.logMessage(logger, message);
        }
    }

    private Logger getLogger(@Nullable String category) {
        @Nullable Logger logger = loggers.get(category);
        return logger == null
            ? defaultLogger
            : logger;
    }

    @Override
    public boolean shouldLog(int level, String category) {
        return super.shouldLog(level, category)
            && Level.fromEclipse(level).isEnabled(getLogger(category));
    }

    enum Level {
        TRACE {
            @Override
            boolean isEnabled(Logger logger) {
                return logger.isTraceEnabled();
            }

            @Override
            void logMessage(Logger logger, String message) {
                logger.trace(message);
            }

            @Override
            void logThrowable(Logger logger, String message, Throwable throwable) {
                logger.trace(message, throwable);
            }
        },

        DEBUG {
            @Override
            boolean isEnabled(Logger logger) {
                return logger.isDebugEnabled();
            }

            @Override
            void logMessage(Logger logger, String message) {
                logger.debug(message);
            }

            @Override
            void logThrowable(Logger logger, String message, Throwable throwable) {
                logger.debug(message, throwable);
            }
        },

        INFO {
            @Override
            boolean isEnabled(Logger logger) {
                return logger.isInfoEnabled();
            }

            @Override
            void logMessage(Logger logger, String message) {
                logger.info(message);
            }

            @Override
            void logThrowable(Logger logger, String message, Throwable throwable) {
                logger.info(message, throwable);
            }
        },

        WARN {
            @Override
            boolean isEnabled(Logger logger) {
                return logger.isWarnEnabled();
            }

            @Override
            void logMessage(Logger logger, String message) {
                logger.warn(message);
            }

            @Override
            void logThrowable(Logger logger, String message, Throwable throwable) {
                logger.warn(message, throwable);
            }
        },

        ERROR {
            @Override
            boolean isEnabled(Logger logger) {
                return logger.isErrorEnabled();
            }

            @Override
            void logMessage(Logger logger, String message) {
                logger.error(message);
            }

            @Override
            void logThrowable(Logger logger, String message, Throwable throwable) {
                logger.error(message, throwable);
            }
        },

        OFF {
            @Override
            boolean isEnabled(Logger logger) {
                return false;
            }

            @Override
            void logMessage(Logger logger, String message) { }

            @Override
            void logThrowable(Logger logger, String message, Throwable throwable) { }
        };

        abstract boolean isEnabled(Logger logger);

        abstract void logMessage(Logger logger, String message);

        abstract void logThrowable(Logger logger, String message, Throwable throwable);

        private static Level fromEclipse(int level) {
            switch (level) {
                case SessionLog.OFF: return OFF;
                case SessionLog.SEVERE: return ERROR;
                case SessionLog.WARNING: return WARN;
                case SessionLog.INFO: return INFO;
                case SessionLog.CONFIG: case SessionLog.FINE: return DEBUG;
                default: return TRACE;
            }
        }
    }
}

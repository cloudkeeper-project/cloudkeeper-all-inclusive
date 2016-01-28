package com.svbio.workflow.service;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.svbio.cloudkeeper.interpreter.EventSubscription;
import com.svbio.cloudkeeper.interpreter.event.ExecutionTraceEvent;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecyclePhase;
import com.svbio.workflow.base.LifecyclePhaseListener;
import com.svbio.workflow.entities.Execution;
import com.svbio.workflow.entities.Execution_;
import com.svbio.workflow.util.SLF4JSessionLog;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import dagger.Module;
import dagger.Provides;
import org.eclipse.persistence.annotations.IdValidation;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.eclipse.persistence.config.SessionCustomizer;
import org.eclipse.persistence.config.TargetServer;
import org.eclipse.persistence.jpa.PersistenceProvider;
import org.eclipse.persistence.logging.SessionLog;
import org.eclipse.persistence.sessions.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Dagger module that enables database logging into the database schema defined in package
 * {@link com.svbio.workflow.entities}.
 */
@Module
final class DatabaseLoggingModule {
    /**
     * Property that determines the JPA persistence provider (see JPA 2.1 §9.2).
     */
    static final String JPA_PROVIDER = "javax.persistence.provider";
    static final String SCHEMA_PROPERTY_NAME = "com.svbio.workflow.database.schema";

    /**
     * Name of the database logging actor.
     *
     * <p>This name is also used by this Dagger module to qualify (using {@link Named}) the {@link ActorRef} that
     * references the database logging actor.
     */
    public static final String DATABASE_LOGGER_NAME = "database-logger";

    /**
     * Number of seconds before {@link ExecutionTraceEvent} messages sent to {@link DatabaseLoggingActor} are discarded
     * if no {@link StartExecutionEvent} has been received in the meantime.
     *
     * @see DatabaseLoggingActor
     */
    private static final long EVICTION_DURATION_SECONDS = 30;

    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    @WorkflowServiceScope
    static EntityManagerFactory provideEntityManagerFactory(DatabaseConfiguration databaseConfiguration,
            LifecycleManager lifecycleManager) {
        final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory(
            Execution.class.getPackage().getName(), databaseConfiguration.properties);
        lifecycleManager.addLifecyclePhaseListener(
            new LifecyclePhaseListener("JPA Entity Manager Factory", LifecyclePhase.STARTED) {
                @Override
                protected void onStop() {
                    entityManagerFactory.close();
                }
            }
        );
        return entityManagerFactory;
    }

    @Provides
    @FirstExecutionIdQualifier
    static long provideFirstExecutionId(EntityManagerFactory entityManagerFactory) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
            CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
            Root<Execution> executionRoot = criteriaQuery.from(Execution.class);
            criteriaQuery.select(criteriaBuilder.max(executionRoot.get(Execution_.id)));
            @Nullable Long queryResult = entityManager.createQuery(criteriaQuery).getSingleResult();
            return queryResult == null
                ? 1
                : queryResult + 1;
        } finally {
            entityManager.close();
        }
    }

    @Provides
    @Named(DATABASE_LOGGER_NAME)
    @WorkflowServiceScope
    static ActorRef provideDatabaseLoggingActor(ActorSystem actorSystem, EntityManagerFactory entityManagerFactory) {
        FiniteDuration evictionDuration = Duration.create(EVICTION_DURATION_SECONDS, TimeUnit.SECONDS);
        return actorSystem.actorOf(
            Props.create(
                new DatabaseLoggingActor.Factory(entityManagerFactory, SystemClock.NANO, evictionDuration)
            ),
            DATABASE_LOGGER_NAME
        );
    }

    @Provides(type = Provides.Type.SET)
    @ExecutionEventQualifier
    static ActorRef provideEventListener(@Named(DATABASE_LOGGER_NAME) ActorRef databaseLoggingActor) {
        return databaseLoggingActor;
    }

    @Provides(type = Provides.Type.SET)
    @InterpreterEventsQualifier
    static EventSubscription provideEventSubscription(@Named(DATABASE_LOGGER_NAME) ActorRef databaseLoggingActor) {
        return new EventSubscription(databaseLoggingActor, ExecutionTraceEvent.class);
    }

    static final class DatabaseConfiguration {
        private final Map<String, String> properties;

        private static void toMap(String keyPrefix, ConfigObject configObject, Map<String, String> map) {
            for (Map.Entry<String, ConfigValue> entry: configObject.entrySet()) {
                String key = keyPrefix + '.' + entry.getKey();
                ConfigValue value = entry.getValue();
                if (value instanceof ConfigObject) {
                    toMap(key, (ConfigObject) value, map);
                } else {
                    map.put(key, value.unwrapped().toString());
                }
            }
        }

        @Inject
        DatabaseConfiguration(Config config) {
            Config dbConfig = config.getConfig("com.svbio.workflow.database");
            String schema = dbConfig.getString("schema");
            Map<String, String> newMap = new LinkedHashMap<>();
            newMap.put(SCHEMA_PROPERTY_NAME, schema);
            // Define that we want to use EclipseLink as JPA persistence provider.
            newMap.put(JPA_PROVIDER, PersistenceProvider.class.getName());
            // Define which primary key components values are considered invalid.
            newMap.put(PersistenceUnitProperties.ID_VALIDATION, IdValidation.NULL.toString());
            // Define class that customizes persistence unit meta-data through Java code.
            newMap.put(PersistenceUnitProperties.SESSION_CUSTOMIZER, SessionCustomizerImpl.class.getName());
            toMap("javax.persistence", dbConfig.getObject("javax.persistence"), newMap);
            newMap.put(PersistenceUnitProperties.LOGGING_LEVEL, SessionLog.ALL_LABEL);
            newMap.put(PersistenceUnitProperties.LOGGING_LOGGER, SLF4JSessionLog.class.getName());
            newMap.put(PersistenceUnitProperties.TARGET_SERVER, TargetServer.None);
            properties = Collections.unmodifiableMap(newMap);
        }
    }

    public static final class SessionCustomizerImpl implements SessionCustomizer {
        private final Logger log = LoggerFactory.getLogger(getClass());

        @Override
        public void customize(@Nullable Session session) {
            assert session != null;
            String schema = (String) session.getProperty(SCHEMA_PROPERTY_NAME);
            session.getLogin().setTableQualifier(schema);
            log.debug("Setting schema for logging tables in database to '{}'.", schema);
        }
    }
}

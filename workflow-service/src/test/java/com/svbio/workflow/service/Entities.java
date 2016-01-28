package com.svbio.workflow.service;

import com.svbio.workflow.entities.Execution;
import com.svbio.workflow.entities.ExecutionFrame;
import com.svbio.workflow.entities.ExecutionFrameError;
import com.svbio.workflow.entities.ExecutionFrameError_;
import com.svbio.workflow.entities.ExecutionFrameProperties;
import com.svbio.workflow.entities.ExecutionFrameProperties_;
import com.svbio.workflow.entities.ExecutionFrame_;
import org.testng.Assert;

import javax.annotation.Nullable;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class Entities {
    private static final long POLLING_INTERVAL_MS = 50;

    private Entities() {
        throw new AssertionError(String.format("No %s instances for you!", getClass().getName()));
    }

    private static List<ExecutionFrame> getExecutionFrames(Execution execution, EntityManager entityManager) {
        CriteriaBuilder executionFrameBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExecutionFrame> executionFrameQuery = executionFrameBuilder.createQuery(ExecutionFrame.class);
        Root<ExecutionFrame> executionFrameRoot = executionFrameQuery.from(ExecutionFrame.class);
        ArrayList<ExecutionFrame> executionFrames = new ArrayList<>(
            entityManager
                .createQuery(
                    executionFrameQuery.where(
                        executionFrameBuilder.equal(executionFrameRoot.get(ExecutionFrame_.execution), execution)
                    )
                )
                .getResultList()
        );
        executionFrames.sort((@Nullable ExecutionFrame left, @Nullable ExecutionFrame right) -> {
            assert left != null && left.getFrame() != null && right != null && right.getFrame() != null;
            return left.getFrame().compareTo(right.getFrame());
        });
        return Collections.unmodifiableList(executionFrames);
    }

    private static List<ExecutionFrameError> getExecutionFrameErrors(Execution execution, EntityManager entityManager) {
        CriteriaBuilder frameErrorBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExecutionFrameError> frameErrorQuery = frameErrorBuilder.createQuery(ExecutionFrameError.class);
        Root<ExecutionFrame> frameRoot = frameErrorQuery.from(ExecutionFrame.class);
        Root<ExecutionFrameError> frameErrorRoot = frameErrorQuery.from(ExecutionFrameError.class);
        return Collections.unmodifiableList(new ArrayList<>(
            entityManager
                .createQuery(
                    frameErrorQuery.where(
                        frameErrorBuilder.equal(
                            frameErrorRoot.get(ExecutionFrameError_.executionFrame), frameRoot
                        ),
                        frameErrorBuilder.equal(
                            frameRoot.get(ExecutionFrame_.execution), execution
                        )
                    )
                )
                .getResultList()
        ));
    }

    private static List<ExecutionFrameProperties<?>> getExecutionFrameProperties(Execution execution,
            EntityManager entityManager) {
        @SuppressWarnings("unchecked")
        Class<ExecutionFrameProperties<?>> clazz
            = (Class<ExecutionFrameProperties<?>>) (Class<?>) ExecutionFrameProperties.class;

        CriteriaBuilder frameErrorBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExecutionFrameProperties<?>> frameErrorQuery = frameErrorBuilder.createQuery(clazz);
        Root<ExecutionFrame> frameRoot = frameErrorQuery.from(ExecutionFrame.class);
        Root<ExecutionFrameProperties<?>> framePropertiesRoot = frameErrorQuery.from(clazz);
        return Collections.unmodifiableList(new ArrayList<>(
            entityManager
                .createQuery(
                    frameErrorQuery.where(
                        frameErrorBuilder.equal(
                            framePropertiesRoot.get(ExecutionFrameProperties_.executionFrame), frameRoot
                        ),
                        frameErrorBuilder.equal(
                            frameRoot.get(ExecutionFrame_.execution), execution
                        )
                    )
                )
                .getResultList()
        ));
    }

    static class TablesContent {
        private final Execution execution;
        private final List<ExecutionFrame> executionFrames;
        private final List<ExecutionFrameError> executionFrameErrors;
        private final List<ExecutionFrameProperties<?>> executionFrameProperties;

        private TablesContent(Execution execution, List<ExecutionFrame> executionFrames,
                List<ExecutionFrameError> executionFrameErrors,
                List<ExecutionFrameProperties<?>> executionFrameProperties) {
            this.execution = execution;
            this.executionFrames = executionFrames;
            this.executionFrameErrors = executionFrameErrors;
            this.executionFrameProperties = executionFrameProperties;
        }

        Execution getExecution() {
            return execution;
        }

        List<ExecutionFrame> getExecutionFrames() {
            return executionFrames;
        }

        List<ExecutionFrameError> getExecutionFrameErrors() {
            return executionFrameErrors;
        }

        List<ExecutionFrameProperties<?>> getExecutionFrameProperties() {
            return executionFrameProperties;
        }
    }

    static TablesContent getTablesContent(long executionId, EntityManagerFactory entityManagerFactory) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        @Nullable Execution execution = entityManager.find(Execution.class, executionId);
        Assert.assertNotNull(execution);
        Assert.assertEquals(execution.getId(), executionId);
        List<ExecutionFrame> executionFrames = getExecutionFrames(execution, entityManager);
        List<ExecutionFrameError> executionFrameErrors = getExecutionFrameErrors(execution, entityManager);
        List<ExecutionFrameProperties<?>> executionFrameProperties = getExecutionFrameProperties(execution, entityManager);
        entityManager.close();
        return new TablesContent(execution, executionFrames, executionFrameErrors, executionFrameProperties);
    }
}

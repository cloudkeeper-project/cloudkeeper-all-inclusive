package com.svbio.workflow.service;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.japi.Option;
import com.svbio.workflow.entities.DRMAAProperties;
import com.svbio.workflow.entities.Execution;
import com.svbio.workflow.entities.ExecutionFrame;
import com.svbio.workflow.entities.ExecutionFrameError;
import com.svbio.workflow.entities.ExecutionFrameProperties;
import com.svbio.workflow.entities.ProcessLauncherProperties;
import com.svbio.workflow.entities.SimpleProperties;
import com.svbio.workflow.entities.UnknownProperties;
import com.svbio.workflow.util.Throwables;
import scala.concurrent.duration.FiniteDuration;
import xyz.cloudkeeper.drm.DrmaaSimpleModuleExecutor;
import xyz.cloudkeeper.executors.ForkedExecutors;
import xyz.cloudkeeper.executors.ForkingExecutor;
import xyz.cloudkeeper.interpreter.event.BeginExecutionTraceEvent;
import xyz.cloudkeeper.interpreter.event.EndExecutionTraceEvent;
import xyz.cloudkeeper.interpreter.event.EndSimpleModuleTraceEvent;
import xyz.cloudkeeper.interpreter.event.Event;
import xyz.cloudkeeper.interpreter.event.ExecutionTraceEvent;
import xyz.cloudkeeper.interpreter.event.FailedExecutionTraceEvent;
import xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor;
import xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult;
import xyz.cloudkeeper.model.immutable.element.Name;
import xyz.cloudkeeper.model.immutable.element.SimpleName;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeCompositeModule;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeCompositeModuleDeclaration;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeInputModule;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeLoopModule;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeModule;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeModuleDeclarationVisitor;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeModuleVisitor;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeProxyModule;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeSimpleModuleDeclaration;
import xyz.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import xyz.cloudkeeper.simple.LocalSimpleModuleExecutor;

import javax.annotation.Nullable;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Akka actor that listens to CloudKeeper interpreter events and logs them to the database.
 *
 * <p>The database logging actor expects to receive two kinds of external messages:
 * <ul><li>
 *     {@link ExecutionTraceEvent} messages coming from the CloudKeeper interpreter. In order to receive these events,
 *     the actor was registered with the CloudKeeper environment using
 *     {@link xyz.cloudkeeper.interpreter.CloudKeeperEnvironmentBuilder#setEventListeners(List)}.
 *     These messages contain execution traces, but since CloudKeeper allows interpretation to happen in a different
 *     JVM, these messages do no contain references to an {@link RuntimeAnnotatedExecutionTrace} instance or any other
 *     linked runtime data structures. Since the database logger also logs information like the kind of a module (for
 *     instance, simple, composite, or loop module), it therefore depends on additional information that the
 *     {@link ExecutionTraceEvent} messages can be "joined" with.
 * </li><li>
 *     {@link StartExecutionEvent} and {@link StopExecutionEvent} messages sent locally (within the current JVM) that
 *     contain the linked runtime data structures needed as described in the previous item.
 * </li></ul>
 *
 * <p>Only the {@link ExecutionTraceEvent} messages are logged in the database, augmented with the additional
 * information provided by the {@link StartExecutionEvent} messages. Since no timing assumptions can be made (messages
 * from different sources may arrive in non-deterministic order), this actor queues {@link ExecutionTraceEvent} messages
 * until a corresponding {@link StartExecutionEvent} is received, or until the {@link ExecutionTraceEvent} message times
 * out. In case of a timeout, the {@link ExecutionTraceEvent} message will be discarded.
 *
 * <p>Note that responding to messages may trigger (runtime) exceptions. How these are handled is not determined by this
 * actor, but is controlled by the parent actor's supervision strategy.
 *
 * @see Event
 */
final class DatabaseLoggingActor extends UntypedActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), (UntypedActor) this);
    private final EntityManagerFactory entityManagerFactory;
    private final Clock clock;
    private final FiniteDuration evictionDuration;
    private final Map<Long, StartExecutionEvent> startExecutionEventMap = new HashMap<>();
    private final Set<Long> endExecutionTraceEventSet = new HashSet<>();
    private final Map<Long, ActorRef> stopExecutionEventMap = new HashMap<>();
    private final LinkedList<QueuedEvent> queuedEvents = new LinkedList<>();

    @Nullable private Cancellable scheduledQueueCleaning = null;

    private DatabaseLoggingActor(Factory factory) {
        entityManagerFactory = factory.entityManagerFactory;
        clock = factory.clock;
        evictionDuration = factory.evictionDuration;
    }

    static class Factory implements Creator<Actor> {
        private static final long serialVersionUID = -1225496895460018463L;

        private final EntityManagerFactory entityManagerFactory;
        private final Clock clock;
        private final FiniteDuration evictionDuration;

        /**
         * Constructs a new creator of database logging actors.
         *
         * @param entityManagerFactory JPA entity manager factory
         * @param evictionDuration duration before
         */
        Factory(EntityManagerFactory entityManagerFactory, Clock clock, FiniteDuration evictionDuration) {
            Objects.requireNonNull(entityManagerFactory);
            Objects.requireNonNull(clock);
            Objects.requireNonNull(evictionDuration);
            this.entityManagerFactory = entityManagerFactory;
            this.clock = clock;
            this.evictionDuration = evictionDuration;
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            throw new NotSerializableException(getClass().getName());
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            throw new NotSerializableException(getClass().getName());
        }

        @Override
        public UntypedActor create() {
            return new DatabaseLoggingActor(this);
        }
    }

    enum ProxyModuleVisitor implements RuntimeModuleDeclarationVisitor<ExecutionFrame.ModuleKind, Void> {
        INSTANCE;

        @Override
        public ExecutionFrame.ModuleKind visit(RuntimeCompositeModuleDeclaration declaration, @Nullable Void ignored) {
            return ExecutionFrame.ModuleKind.COMPOSITE;
        }

        @Override
        public ExecutionFrame.ModuleKind visit(RuntimeSimpleModuleDeclaration declaration, @Nullable Void ignored) {
            return ExecutionFrame.ModuleKind.SIMPLE;
        }
    }

    enum ModuleVisitor implements RuntimeModuleVisitor<ExecutionFrame.ModuleKind, Void> {
        INSTANCE;

        @Override
        public ExecutionFrame.ModuleKind visit(RuntimeInputModule module, @Nullable Void ignored) {
            return ExecutionFrame.ModuleKind.INPUT;
        }

        @Override
        public ExecutionFrame.ModuleKind visit(RuntimeCompositeModule module, @Nullable Void ignored) {
            return ExecutionFrame.ModuleKind.COMPOSITE;
        }

        @Override
        public ExecutionFrame.ModuleKind visit(RuntimeLoopModule module, @Nullable Void ignored) {
            return ExecutionFrame.ModuleKind.LOOP;
        }

        @Override
        public ExecutionFrame.ModuleKind visit(RuntimeProxyModule module, @Nullable Void ignored) {
            return module.getDeclaration().accept(ProxyModuleVisitor.INSTANCE, null);
        }
    }

    @Nullable
    private static Date time(SimpleModuleExecutorResult result, Class<?> clazz, SimpleName propertyName) {
        return time(result, Name.qualifiedName(clazz.getName()), propertyName);
    }

    @Nullable
    private static Date time(SimpleModuleExecutorResult result, Name executorName, SimpleName propertyName) {
        @Nullable Long millis = result.getProperty(Long.class, executorName, propertyName);
        return millis == null
            ? null
            : new Date(millis);
    }

    @Nullable
    private static Integer integer(SimpleModuleExecutorResult result, Class<?> clazz, SimpleName propertyName) {
        @Nullable Long number = result.getProperty(Long.class, Name.qualifiedName(clazz.getName()), propertyName);
        return number == null
            ? null
            : (int) (long) number;
    }

    private static void processEndExecutionTraceEvent(ExecutionFrame executionFrame,
            EntityManager entityManager, SimpleModuleExecutorResult result) {
        @Nullable ProcessLauncherProperties<?> processLauncherProperties = null;
        Name executorName = result.getExecutorName();
        if (executorName.contentEquals(DrmaaSimpleModuleExecutor.class.getName())) {
            processLauncherProperties = new DRMAAProperties()
                .setDrmaaJobId(result.getProperty(String.class, executorName, DrmaaSimpleModuleExecutor.JOB_ID))
                .setNativeArguments(
                    result.getProperty(String.class, executorName, DrmaaSimpleModuleExecutor.NATIVE_ARGUMENTS)
                )
                .setCommandLine(
                    result.getProperty(String.class, executorName, DrmaaSimpleModuleExecutor.COMMAND_LINE)
                )
                .setExitStatus(integer(result, DrmaaSimpleModuleExecutor.class, DrmaaSimpleModuleExecutor.EXIT_VALUE));
        } else if (executorName.contentEquals(ForkingExecutor.class.getName())) {
            processLauncherProperties = new SimpleProperties()
                .setCommandLine(result.getProperty(String.class, executorName, ForkingExecutor.COMMAND_LINE))
                .setExitStatus(integer(result, ForkingExecutor.class, ForkingExecutor.EXIT_VALUE));
        }

        ExecutionFrameProperties<?> executionFrameProperties;
        if (processLauncherProperties != null) {
            processLauncherProperties
                .setLauncherStartTime(time(result, ForkedExecutors.class, SimpleModuleExecutor.SUBMISSION_TIME_MILLIS))
                .setLauncherFinishTime(time(result, ForkedExecutors.class, SimpleModuleExecutor.COMPLETION_TIME_MILLIS))
                .setSimpleModuleStartTime(time(
                    result, LocalSimpleModuleExecutor.class, LocalSimpleModuleExecutor.PROCESSING_START_TIME_MILLIS))
                .setSimpleModuleFinishTime(time(
                    result, LocalSimpleModuleExecutor.class, LocalSimpleModuleExecutor.PROCESSING_FINISH_TIME_MILLIS));
            executionFrameProperties = processLauncherProperties;
        } else {
            executionFrameProperties = new UnknownProperties();
        }

        executionFrameProperties
            .setExecutionFrame(executionFrame)
            .setWorkerName(executorName.toString())
            .setWorkerStartTime(time(result, executorName, SimpleModuleExecutor.SUBMISSION_TIME_MILLIS))
            .setWorkerFinishTime(time(result, executorName, SimpleModuleExecutor.COMPLETION_TIME_MILLIS));

        entityManager.persist(executionFrameProperties);
    }

    private static void processEvent(ExecutionTraceEvent event, EntityManager entityManager, String prefix,
            RuntimeModule module) {
        Date timestamp = new Date(event.getTimestamp());
        @Nullable Execution execution = entityManager.find(Execution.class, event.getExecutionId());
        if (execution == null) {
            execution = new Execution()
                .setId(event.getExecutionId())
                .setKeyPrefix(prefix);
            entityManager.persist(execution);
        }

        String executionTraceString = event.getExecutionTrace().toString();
        @Nullable ExecutionFrame executionFrame = entityManager.find(
            ExecutionFrame.class,
            new ExecutionFrame.ID()
                .setExecution(execution.getId())
                .setFrame(executionTraceString)
        );
        if (executionFrame == null) {
            executionFrame = new ExecutionFrame()
                .setExecution(execution)
                .setFrame(executionTraceString)
                .setModuleKind(module.accept(ModuleVisitor.INSTANCE, null));
            entityManager.persist(executionFrame);
        }

        if (event instanceof BeginExecutionTraceEvent) {
            if (event.getExecutionTrace().isEmpty()) {
                execution.setStartTime(timestamp);
            }
            executionFrame.setStartTime(timestamp);
        } else if (event instanceof EndExecutionTraceEvent) {
            if (event.getExecutionTrace().isEmpty()) {
                execution.setFinishTime(timestamp);
            }
            EndExecutionTraceEvent endExecutionTraceEvent = (EndExecutionTraceEvent) event;

            executionFrame.setFinishTime(timestamp);
            executionFrame.setSuccessful(endExecutionTraceEvent.isSuccessful());

            if (event instanceof EndSimpleModuleTraceEvent) {
                Option<SimpleModuleExecutorResult> optionalResult
                    = ((EndSimpleModuleTraceEvent) event).getModuleExecutorResult();
                if (optionalResult.isDefined()) {
                    processEndExecutionTraceEvent(executionFrame, entityManager, optionalResult.get());
                }
            }
        } else if (event instanceof FailedExecutionTraceEvent) {
            FailedExecutionTraceEvent failedExecutionTraceEvent = (FailedExecutionTraceEvent) event;
            entityManager.persist(
                new ExecutionFrameError()
                    .setExecutionFrame(executionFrame)
                    .setErrorMessage(Throwables.executionTraceToString(failedExecutionTraceEvent.getException()))
            );
        }
    }

    private void processEventImmediately(ExecutionTraceEvent event, StartExecutionEvent startExecutionEvent) {
        RuntimeAnnotatedExecutionTrace annotatedTrace
            = startExecutionEvent.getRootTrace().resolveExecutionTrace(event.getExecutionTrace());
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            processEvent(event, entityManager, startExecutionEvent.getPrefix(), annotatedTrace.getModule());
            transaction.commit();
        } finally {
            entityManager.close();
        }
    }

    /**
     * Schedules a queue cleaning of {@link #queuedEvents}, or does nothing if a queue cleaning is already scheduled.
     */
    private void scheduleQueueCleaning() {
        if (scheduledQueueCleaning == null) {
            scheduledQueueCleaning = getContext().system().scheduler().scheduleOnce(
                evictionDuration,
                getSelf(),
                LocalMessages.CLEAN_QUEUE,
                getContext().dispatcher(),
                getSelf()
            );
        }
    }

    private void discardCurrentQueuedEvent(QueuedEvent event, Iterator<QueuedEvent> iterator) {
        log.debug(
            "Discarding {} because a corresponding {} message was not received within {} ms.",
            event.event, StartExecutionEvent.class.getSimpleName(), evictionDuration.toMillis()
        );
        iterator.remove();
    }

    /**
     * Process events that are queued in {@link #queuedEvents}.
     *
     * <p>While going through the list of queued events, this method also cleans them (in the same fashion as
     * {@link #cleanQueue}). If the queue is not empty, another queue cleaning is scheduled.
     */
    private void processQueuedEvents() {
        if (scheduledQueueCleaning != null) {
            scheduledQueueCleaning.cancel();
            scheduledQueueCleaning = null;
        }

        long currentTimestamp = clock.getCurrentTime();
        TimeUnit timeUnit = clock.getTimeUnit();
        Iterator<QueuedEvent> it = queuedEvents.iterator();
        while (it.hasNext()) {
            QueuedEvent queuedEvent = it.next();
            if (timeUnit.toNanos(currentTimestamp - queuedEvent.timestamp) > evictionDuration.toNanos()) {
                discardCurrentQueuedEvent(queuedEvent, it);
            } else {
                @Nullable StartExecutionEvent startExecutionEvent
                    = startExecutionEventMap.get(queuedEvent.event.getExecutionId());
                if (startExecutionEvent == null) {
                    // The queue is not empty, so make sure another queue cleaning is scheduled.
                    scheduleQueueCleaning();
                } else {
                    processEventImmediately(queuedEvent.event, startExecutionEvent);
                    it.remove();
                }
            }
        }
    }

    /**
     * Handles scheduled queue-cleaning event ({@link LocalMessages#CLEAN_QUEUE}).
     *
     * <p>If the queue is not empty after the cleaning, another queue cleaning is scheduled.
     */
    private void cleanQueue() {
        scheduledQueueCleaning = null;

        long currentTimestamp = clock.getCurrentTime();
        TimeUnit timeUnit = clock.getTimeUnit();
        Iterator<QueuedEvent> it = queuedEvents.iterator();
        while (it.hasNext()) {
            QueuedEvent queuedEvent = it.next();
            if (timeUnit.toNanos(currentTimestamp - queuedEvent.timestamp) > evictionDuration.toNanos()) {
                discardCurrentQueuedEvent(queuedEvent, it);
            } else {
                // The queue is not empty, so make sure another queue cleaning is scheduled.
                scheduleQueueCleaning();
                // queuedEvents is sorted by timestamp, so we can stop here
                break;
            }
        }
        // Queue is now empty, no need to schedule another cleaning
    }

    /**
     * Handles CloudKeeper interpreter {@link EndExecutionTraceEvent}.
     *
     * @param event the event
     */
    private void handleEndExecutionTraceEvent(EndExecutionTraceEvent event) {
        handleExecutionTraceEvent(event);

        if (event.getExecutionTrace().isEmpty()) {
            long executionId = event.getExecutionId();
            endExecutionTraceEventSet.add(executionId);
            tryRemoveExecutionId(executionId);
        }
    }

    /**
     * Handles CloudKeeper interpreter event pertaining to an execution trace.
     *
     * @param event the event
     */
    private void handleExecutionTraceEvent(ExecutionTraceEvent event) {
        long executionId = event.getExecutionId();
        @Nullable StartExecutionEvent startExecutionEvent = startExecutionEventMap.get(executionId);
        if (startExecutionEvent == null) {
            log.debug("Queuing {} because execution id {} is currently unknown.", event, executionId);
            queuedEvents.add(new QueuedEvent(clock.getCurrentTime(), event));
            scheduleQueueCleaning();
        } else {
            processEventImmediately(event, startExecutionEvent);
        }
    }

    /**
     * Handles CloudKeeper service event that a workflow execution has started.
     *
     * <p>There are no ordering guarantees with respect to CloudKeeper interpreter events (such as
     * {@link BeginExecutionTraceEvent}).
     */
    private void handleStartWorkflowExecutionEvent(StartExecutionEvent startExecutionEvent) {
        startExecutionEventMap.put(startExecutionEvent.getExecutionId(), startExecutionEvent);
        processQueuedEvents();
    }

    /**
     * Handles CloudKeeper service event that a workflow execution has ended.
     *
     * <p>There are no ordering guarantees with respect to CloudKeeper interpreter events (such as
     * {@link EndExecutionTraceEvent}). Therefore, the execution is only removed from
     * {@link #startExecutionEventMap} after a delay.
     *
     * @see #removeExecutionId(long)
     */
    private void handleFinishWorkflowExecutionEvent(long executionId) {
        stopExecutionEventMap.put(executionId, getSender());
        tryRemoveExecutionId(executionId);

        // Schedule a removal regardless of whether also the EndExecutionTraceEvent has been received. This only serves
        // as fallback if tryRemoveExecutionId did not actually call removeExecutionId(long).
        getContext().system().scheduler().scheduleOnce(
            evictionDuration,
            getSelf(),
            new RemoveExecutionId(executionId),
            getContext().dispatcher(),
            getSelf()
        );
    }

    private void tryRemoveExecutionId(long executionId) {
        if (endExecutionTraceEventSet.contains(executionId) && stopExecutionEventMap.containsKey(executionId)) {
            removeExecutionId(executionId);
        }
    }

    /**
     * Removes an execution id from {@link #startExecutionEventMap} as the result of a previous
     * {@link StopExecutionEvent} message.
     *
     * @see #handleFinishWorkflowExecutionEvent(long)
     */
    private void removeExecutionId(long executionId) {
        startExecutionEventMap.remove(executionId);
        endExecutionTraceEventSet.remove(executionId);
        @Nullable ActorRef stopExecutionEventSender = stopExecutionEventMap.remove(executionId);
        if (stopExecutionEventSender != null) {
            stopExecutionEventSender.tell(executionId, getSelf());
        }
    }

    @Override
    public void onReceive(Object message) {
        if (message instanceof EndExecutionTraceEvent) {
            handleEndExecutionTraceEvent((EndExecutionTraceEvent) message);
        } else if (message instanceof ExecutionTraceEvent) {
            handleExecutionTraceEvent((ExecutionTraceEvent) message);
        } else if (message instanceof Event) {
            log.debug("Ignoring unexpected {}", message);
        } else if (message instanceof StartExecutionEvent) {
            handleStartWorkflowExecutionEvent((StartExecutionEvent) message);
        } else if (message instanceof StopExecutionEvent) {
            handleFinishWorkflowExecutionEvent(((ExecutionEvent) message).getExecutionId());
        } else if (message instanceof RemoveExecutionId) {
            removeExecutionId(((RemoveExecutionId) message).executionId);
        } else if (message == LocalMessages.CLEAN_QUEUE) {
            cleanQueue();
        } else {
            unhandled(message);
        }
    }

    /**
     * Returns the list of queued events.
     *
     * <p>This method only exists for unit-testing purposes. It should never be called elsewhere.
     */
    List<QueuedEvent> getQueuedEvents() {
        return Collections.unmodifiableList(queuedEvents);
    }

    /**
     * Returns the execution map.
     *
     * <p>This method only exists for unit-testing purposes. It should never be called elsewhere.
     */
    Map<Long, StartExecutionEvent> getStartExecutionEventMap() {
        return Collections.unmodifiableMap(startExecutionEventMap);
    }

    /**
     * Class that wraps a CloudKeeper interpreter event together with a timestamp (returned by
     * {@link Clock#getCurrentTime()}).
     *
     * <p>An event is queued in {@link #handleExecutionTraceEvent(ExecutionTraceEvent)} if at the time it is received
     * there is no entry in {@link #startExecutionEventMap} for that execution id. This should usually only happen if
     * the corresponding {@link StartExecutionEvent} was not processed yet (though it could theoretically also happen if
     * some messages got lost, for instance, due to heavy machine load).
     */
    static final class QueuedEvent {
        private final long timestamp;
        private final ExecutionTraceEvent event;

        private QueuedEvent(long timestamp, ExecutionTraceEvent event) {
            this.timestamp = timestamp;
            this.event = event;
        }

        /**
         * Returns the {@link ExecutionTraceEvent} instance.
         *
         * <p>This method only exists for unit-testing purposes. It should never be called elsewhere.
         */
        ExecutionTraceEvent getEvent() {
            return event;
        }
    }

    private enum LocalMessages {
        /**
         * Message to trigger {@link #cleanQueue()}.
         */
        CLEAN_QUEUE
    }

    private static final class RemoveExecutionId {
        private final long executionId;

        RemoveExecutionId(long executionId) {
            this.executionId = executionId;
        }
    }
}

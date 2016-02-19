package com.svbio.workflow.service;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import com.svbio.workflow.entities.DRMAAProperties;
import com.svbio.workflow.entities.Execution;
import com.svbio.workflow.entities.ExecutionFrame;
import com.svbio.workflow.entities.ExecutionFrameError;
import com.svbio.workflow.entities.ProcessLauncherProperties;
import com.svbio.workflow.entities.SimpleProperties;
import com.svbio.workflow.service.DatabaseLoggingActor.QueuedEvent;
import com.svbio.workflow.util.SLF4JSessionLog;
import com.svbio.workflow.util.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.eclipse.persistence.config.TargetServer;
import org.eclipse.persistence.logging.SessionLog;
import org.h2.Driver;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import xyz.cloudkeeper.drm.DrmaaSimpleModuleExecutor;
import xyz.cloudkeeper.examples.modules.BinarySum;
import xyz.cloudkeeper.examples.modules.Fibonacci;
import xyz.cloudkeeper.examples.repositories.FibonacciRepository;
import xyz.cloudkeeper.examples.repositories.SimpleRepository;
import xyz.cloudkeeper.executors.ForkedExecutors;
import xyz.cloudkeeper.executors.ForkingExecutor;
import xyz.cloudkeeper.interpreter.InterpreterException;
import xyz.cloudkeeper.interpreter.event.BeginExecutionTraceEvent;
import xyz.cloudkeeper.interpreter.event.EndExecutionTraceEvent;
import xyz.cloudkeeper.interpreter.event.EndSimpleModuleTraceEvent;
import xyz.cloudkeeper.interpreter.event.FailedExecutionTraceEvent;
import xyz.cloudkeeper.linker.Linker;
import xyz.cloudkeeper.linker.LinkerOptions;
import xyz.cloudkeeper.model.LinkerException;
import xyz.cloudkeeper.model.api.ExecutionException;
import xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor;
import xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult;
import xyz.cloudkeeper.model.bare.execution.BareOverride;
import xyz.cloudkeeper.model.beans.element.module.MutableProxyModule;
import xyz.cloudkeeper.model.immutable.element.Name;
import xyz.cloudkeeper.model.immutable.execution.ExecutionTrace;
import xyz.cloudkeeper.model.runtime.element.RuntimeRepository;
import xyz.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import xyz.cloudkeeper.simple.LocalSimpleModuleExecutor;

import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ITDatabaseLoggingActor {
    private static final FiniteDuration EVICTION_DURATION = Duration.create(100, TimeUnit.MILLISECONDS);

    @Nullable private ActorSystem actorSystem;
    @Nullable private MockScheduler scheduler;
    @Nullable private EntityManagerFactory entityManagerFactory;
    @Nullable private LinkerOptions linkerOptions;
    @Nullable private RuntimeRepository repository;

    @BeforeClass
    public void setup() throws ClassNotFoundException, LinkerException {
        Config config = ConfigFactory
            .parseMap(Collections.singletonMap(
                "akka.scheduler.implementation", MockScheduler.class.getName()
            ))
            .withFallback(ConfigFactory.load());
        actorSystem = ActorSystem.create(getClass().getSimpleName(), config);
        scheduler = (MockScheduler) actorSystem.scheduler();

        Map<String, String> javaPersistenceProperties = new LinkedHashMap<>();
        javaPersistenceProperties.put("javax.persistence.jdbc.driver", Driver.class.getName());
        javaPersistenceProperties.put("javax.persistence.jdbc.url", "jdbc:h2:mem:" + getClass().getSimpleName());
        javaPersistenceProperties.put("javax.persistence.schema-generation.database.action", "create");
        // EclipseLink properties
        javaPersistenceProperties.put(PersistenceUnitProperties.LOGGING_LEVEL, SessionLog.ALL_LABEL);
        javaPersistenceProperties.put(PersistenceUnitProperties.LOGGING_LOGGER, SLF4JSessionLog.class.getName());
        javaPersistenceProperties.put(PersistenceUnitProperties.TARGET_SERVER, TargetServer.None);
        entityManagerFactory
            = Persistence.createEntityManagerFactory(Execution.class.getPackage().getName(), javaPersistenceProperties);

        linkerOptions = LinkerOptions.nonExecutable();
        repository = Linker.createRepository(
            Arrays.asList(new FibonacciRepository().get(), new SimpleRepository().get()), linkerOptions
        );
    }

    @AfterClass
    public void tearDown() {
        assert entityManagerFactory != null && actorSystem != null;
        entityManagerFactory.close();
        JavaTestKit.shutdownActorSystem(actorSystem);
    }

    @Test
    public void testEviction() throws InterruptedException, LinkerException {
        assert actorSystem != null && entityManagerFactory != null && scheduler != null && repository != null
            && linkerOptions != null;
        TestActorRef<DatabaseLoggingActor> actorRef = TestActorRef.create(
            actorSystem,
            Props.create(new DatabaseLoggingActor.Factory(entityManagerFactory, scheduler, EVICTION_DURATION)),
            "testEviction database logger"
        );
        DatabaseLoggingActor actor = actorRef.underlyingActor();
        List<QueuedEvent> queuedEvents = actor.getQueuedEvents();
        Map<Long, StartExecutionEvent> executionMap = actor.getStartExecutionEventMap();

        RuntimeAnnotatedExecutionTrace fibonacciRootTrace = Linker.createAnnotatedExecutionTrace(
            ExecutionTrace.empty(),
            new MutableProxyModule().setDeclaration(Fibonacci.class.getName()),
            Collections.<BareOverride>emptyList(),
            repository,
            linkerOptions
        );

        long baseTime = System.currentTimeMillis();
        BeginExecutionTraceEvent beginFibonacciEvent
            = BeginExecutionTraceEvent.of(0, baseTime, ExecutionTrace.empty());
        BeginExecutionTraceEvent beginLoopEvent
            = BeginExecutionTraceEvent.of(0, baseTime + 1, ExecutionTrace.valueOf("/loop"));

        actorRef.tell(beginFibonacciEvent, ActorRef.noSender());
        actorRef.tell(beginLoopEvent, ActorRef.noSender());
        Assert.assertEquals(queuedEvents.size(), 2);
        Assert.assertSame(queuedEvents.get(0).getEvent(), beginFibonacciEvent);
        Assert.assertSame(queuedEvents.get(1).getEvent(), beginLoopEvent);
        Assert.assertEquals(executionMap, Collections.emptyMap());
        scheduler.advanceTime(EVICTION_DURATION.toMillis());
        Assert.assertEquals(queuedEvents.size(), 2);
        scheduler.advanceTime(1);
        Assert.assertEquals(queuedEvents, Collections.emptyList());
        Assert.assertEquals(executionMap, Collections.emptyMap());

        StartExecutionEvent startExecutionEvent = new StartExecutionEvent(0, fibonacciRootTrace, "testEviction");
        actorRef.tell(startExecutionEvent, ActorRef.noSender());
        Assert.assertEquals(queuedEvents, Collections.emptyList());
        Assert.assertEquals(executionMap, Collections.singletonMap(0L, startExecutionEvent));

        actorRef.tell(BeginExecutionTraceEvent.of(0, baseTime + 2, ExecutionTrace.valueOf("/loop/0")),
            ActorRef.noSender());
        Assert.assertEquals(queuedEvents, Collections.emptyList());
        Assert.assertEquals(executionMap, Collections.singletonMap(0L, startExecutionEvent));

        actorRef.tell(new StopExecutionEvent(0), ActorRef.noSender());
        Assert.assertEquals(queuedEvents, Collections.emptyList());
        Assert.assertEquals(executionMap, Collections.singletonMap(0L, startExecutionEvent));
        // The execution is removed only after a grace period.
        scheduler.advanceTime(EVICTION_DURATION.toMillis() + 1);
        Assert.assertEquals(executionMap, Collections.emptyMap());

        EndExecutionTraceEvent endLoopIteration0Event
            = EndExecutionTraceEvent.of(0, baseTime + 2, ExecutionTrace.valueOf("/loop/0"), true);
        actorRef.tell(endLoopIteration0Event, ActorRef.noSender());
        Assert.assertEquals(queuedEvents.size(), 1);
        Assert.assertSame(queuedEvents.get(0).getEvent(), endLoopIteration0Event);
        Assert.assertEquals(executionMap, Collections.emptyMap());

        scheduler.advanceTime(EVICTION_DURATION.toMillis() + 1);
        Assert.assertEquals(queuedEvents, Collections.emptyList());
        Assert.assertEquals(executionMap, Collections.emptyMap());
    }

    /**
     * Delta (in ms) to the timestamp of the corresponding {@link BeginExecutionTraceEvent}.
     */
    private static final int SUBMISSION_TIME_DELTA = 10;
    private static final int FORKED_JVM_START_DELTA = 20;
    private static final int FORKED_JVM_SUBMISSION_DELTA = 30;
    private static final int FORKED_JVM_START_RUN_METHOD_DELTA = 40;
    private static final int FORKED_JVM_END_RUN_METHOD_DELTA = 50;
    private static final int FORKED_JVM_COMPLETION_DELTA = 60;
    private static final int FORKED_JVM_TERMINATION_DELTA = 70;
    private static final int COMPLETION_TIME_DELTA = 80;
    private static final int END_EVENT_DELTA = 90;

    private static SimpleModuleExecutorResult.Builder endEventBuilder(
            Class<? extends SimpleModuleExecutor> executorClass, long baseTime,
            @Nullable ExecutionException exception) {
        return new SimpleModuleExecutorResult.Builder(Name.qualifiedName(executorClass.getName()))
            .addProperty(SimpleModuleExecutor.SUBMISSION_TIME_MILLIS, baseTime + SUBMISSION_TIME_DELTA)
            .addExecutionResult(
                new SimpleModuleExecutorResult.Builder(Name.qualifiedName(ForkedExecutors.class.getName()))
                    .addProperty(SimpleModuleExecutor.SUBMISSION_TIME_MILLIS, baseTime + FORKED_JVM_START_DELTA)
                    .addExecutionResult(
                        new SimpleModuleExecutorResult.Builder(
                            Name.qualifiedName(LocalSimpleModuleExecutor.class.getName()))
                            .addProperty(SimpleModuleExecutor.SUBMISSION_TIME_MILLIS,
                                baseTime + FORKED_JVM_SUBMISSION_DELTA)
                            .addProperty(LocalSimpleModuleExecutor.PROCESSING_START_TIME_MILLIS,
                                baseTime + FORKED_JVM_START_RUN_METHOD_DELTA)
                            .addProperty(LocalSimpleModuleExecutor.PROCESSING_FINISH_TIME_MILLIS,
                                baseTime + FORKED_JVM_END_RUN_METHOD_DELTA)
                            .addProperty(SimpleModuleExecutor.COMPLETION_TIME_MILLIS,
                                baseTime + FORKED_JVM_COMPLETION_DELTA)
                            .build()
                    )
                    .addProperty(SimpleModuleExecutor.COMPLETION_TIME_MILLIS, baseTime + FORKED_JVM_TERMINATION_DELTA)
                    .build()
            )
            .addProperty(SimpleModuleExecutor.COMPLETION_TIME_MILLIS, baseTime + COMPLETION_TIME_DELTA)
            .setException(exception);
    }

    private static EndSimpleModuleTraceEvent fabricatedDRMMAEndEvent(long executionId, ExecutionTrace executionTrace,
            long baseTime, String jobId, String commandLine, int exitValue, @Nullable ExecutionException exception) {
        return EndSimpleModuleTraceEvent.of(
            executionId, baseTime + END_EVENT_DELTA, executionTrace,
            endEventBuilder(DrmaaSimpleModuleExecutor.class, baseTime, exception)
                .addProperty(DrmaaSimpleModuleExecutor.JOB_ID, jobId)
                .addProperty(DrmaaSimpleModuleExecutor.NATIVE_ARGUMENTS, "-v")
                .addProperty(DrmaaSimpleModuleExecutor.COMMAND_LINE, commandLine)
                .addProperty(DrmaaSimpleModuleExecutor.EXIT_VALUE, (long) exitValue)
                .build()
        );
    }

    private static void setExecutionProperties(Class<? extends SimpleModuleExecutor> executorClass,
            ProcessLauncherProperties<?> properties, ExecutionFrame executionFrame, long baseTime, String commandLine,
            int exitValue) {
        properties
            .setExecutionFrame(executionFrame)
            .setCommandLine(commandLine)
            .setExitStatus(exitValue)
            .setWorkerName(executorClass.getName())
            .setWorkerStartTime(new Date(baseTime + SUBMISSION_TIME_DELTA))
            .setLauncherStartTime(new Date(baseTime + FORKED_JVM_START_DELTA))
            .setSimpleModuleStartTime(new Date(baseTime + FORKED_JVM_START_RUN_METHOD_DELTA))
            .setSimpleModuleFinishTime(new Date(baseTime + FORKED_JVM_END_RUN_METHOD_DELTA))
            .setLauncherFinishTime(new Date(baseTime + FORKED_JVM_TERMINATION_DELTA))
            .setWorkerFinishTime(new Date(baseTime + COMPLETION_TIME_DELTA));
    }

    private static DRMAAProperties fabricatedDrmaaProperties(ExecutionFrame executionFrame, long baseTime,
            String jobId, String commandLine, int exitValue) {
        DRMAAProperties drmaaProperties = new DRMAAProperties()
            .setDrmaaJobId(jobId)
            .setNativeArguments("-v");
        setExecutionProperties(DrmaaSimpleModuleExecutor.class, drmaaProperties, executionFrame, baseTime, commandLine,
            exitValue);
        return drmaaProperties;
    }

    private static EndSimpleModuleTraceEvent fabricatedForkingEndEvent(long executionId, ExecutionTrace executionTrace,
            long baseTime, String commandLine, int exitValue, ExecutionException exception) {
        return EndSimpleModuleTraceEvent.of(
            executionId, baseTime + END_EVENT_DELTA, executionTrace,
            endEventBuilder(ForkingExecutor.class, baseTime, exception)
                .addProperty(ForkingExecutor.COMMAND_LINE, commandLine)
                .addProperty(ForkingExecutor.EXIT_VALUE, (long) exitValue)
                .build()
        );
    }

    private static SimpleProperties fabricatedForkingProperties(ExecutionFrame executionFrame, long baseTime,
            String commandLine, int exitValue) {
        SimpleProperties simpleProperties = new SimpleProperties();
        setExecutionProperties(ForkingExecutor.class, simpleProperties, executionFrame, baseTime, commandLine,
            exitValue);
        return simpleProperties;
    }

    private static void sendTo(ActorRef actor, Object message) {
        actor.tell(message, ActorRef.noSender());
    }

    @Test
    public void testLogging() throws Exception {
        assert entityManagerFactory != null && scheduler != null && repository != null && linkerOptions != null;
        long executionId = 1;
        TestActorRef<DatabaseLoggingActor> actorRef = TestActorRef.create(
            actorSystem,
            Props.create(new DatabaseLoggingActor.Factory(entityManagerFactory, scheduler, EVICTION_DURATION)),
            "testLogging database logger"
        );
        RuntimeAnnotatedExecutionTrace fibonacciRootTrace = Linker.createAnnotatedExecutionTrace(
            ExecutionTrace.empty(),
            new MutableProxyModule().setDeclaration(Fibonacci.class.getName()),
            Collections.<BareOverride>emptyList(),
            repository,
            linkerOptions
        );

        long baseTime = System.currentTimeMillis();
        sendTo(actorRef, BeginExecutionTraceEvent.of(executionId, baseTime, ExecutionTrace.empty()));
        sendTo(actorRef, BeginExecutionTraceEvent.of(executionId, baseTime + 100, ExecutionTrace.valueOf("/loop")));
        sendTo(actorRef, BeginExecutionTraceEvent.of(executionId, baseTime + 200, ExecutionTrace.valueOf("/loop/1")));

        ExecutionTrace sumExecutionTrace =  ExecutionTrace.valueOf("/loop/1/sum");
        sendTo(actorRef, BeginExecutionTraceEvent.of(executionId, baseTime + 300, sumExecutionTrace));
        sendTo(
            actorRef,
            fabricatedDRMMAEndEvent(executionId, sumExecutionTrace, baseTime + 300, "drmaa1", "foo bar", 0, null)
        );

        ExecutionTrace decrExecutionTrace = ExecutionTrace.valueOf("/loop/1/decr");
        sendTo(actorRef, BeginExecutionTraceEvent.of(executionId, baseTime + 400, decrExecutionTrace));
        ExecutionException executionException = new ExecutionException("Lorem ipsum");
        sendTo(
            actorRef,
            fabricatedDRMMAEndEvent(
                executionId, decrExecutionTrace, baseTime + 400, "drmaa2", "foo baz", 1, executionException)
        );
        sendTo(actorRef, EndExecutionTraceEvent.of(executionId, baseTime + 500, ExecutionTrace.empty(), false));
        InterpreterException interpreterException
            = new InterpreterException(decrExecutionTrace, "Fabricated exception.", executionException);
        sendTo(actorRef, FailedExecutionTraceEvent.of(executionId, baseTime + 500, decrExecutionTrace,
            interpreterException));

        sendTo(actorRef, new StartExecutionEvent(executionId, fibonacciRootTrace, "testLogging"));
        sendTo(actorRef, new StopExecutionEvent(executionId));

        Entities.TablesContent tablesContent = Entities.getTablesContent(executionId, entityManagerFactory);
        Assert.assertEquals(
            tablesContent.getExecution(),
            new Execution()
                .setId(executionId)
                .setStartTime(new Date(baseTime))
                .setFinishTime(new Date(baseTime + 500))
                .setKeyPrefix("testLogging")
        );
        ExecutionFrame expectedSumExecutionFrame = new ExecutionFrame()
            .setExecution(tablesContent.getExecution())
            .setFrame(sumExecutionTrace.toString())
            .setStartTime(new Date(baseTime + 300))
            .setFinishTime(new Date(baseTime + 300 + END_EVENT_DELTA))
            .setModuleKind(ExecutionFrame.ModuleKind.SIMPLE)
            .setSuccessful(true);
        ExecutionFrame expectedDecrExecutionFrame = new ExecutionFrame()
            .setExecution(tablesContent.getExecution())
            .setFrame(decrExecutionTrace.toString())
            .setStartTime(new Date(baseTime + 400))
            .setFinishTime(new Date(baseTime + 400 + END_EVENT_DELTA))
            .setModuleKind(ExecutionFrame.ModuleKind.SIMPLE)
            .setSuccessful(false);
        Assert.assertEquals(
            tablesContent.getExecutionFrames(),
            Arrays.asList(
                new ExecutionFrame()
                    .setExecution(tablesContent.getExecution())
                    .setFrame("")
                    .setStartTime(new Date(baseTime))
                    .setFinishTime(new Date(baseTime + 500))
                    .setModuleKind(ExecutionFrame.ModuleKind.COMPOSITE)
                    .setSuccessful(false),
                new ExecutionFrame()
                    .setExecution(tablesContent.getExecution())
                    .setFrame("/loop")
                    .setStartTime(new Date(baseTime + 100))
                    .setModuleKind(ExecutionFrame.ModuleKind.LOOP),
                new ExecutionFrame()
                    .setExecution(tablesContent.getExecution())
                    .setFrame("/loop/1")
                    .setStartTime(new Date(baseTime + 200))
                    .setModuleKind(ExecutionFrame.ModuleKind.LOOP),
                expectedDecrExecutionFrame,
                expectedSumExecutionFrame
            )
        );
        Assert.assertEquals(
            tablesContent.getExecutionFrameProperties(),
            Arrays.asList(
                fabricatedDrmaaProperties(expectedSumExecutionFrame, baseTime + 300, "drmaa1", "foo bar", 0),
                fabricatedDrmaaProperties(expectedDecrExecutionFrame, baseTime + 400, "drmaa2", "foo baz", 1)
            )
        );
        Assert.assertEquals(
            tablesContent.getExecutionFrameErrors(),
            Collections.singletonList(
                new ExecutionFrameError()
                    .setExecutionFrame(expectedDecrExecutionFrame)
                    .setErrorMessage(Throwables.executionTraceToString(interpreterException))
            )
        );
    }

    /**
     * Tests logging if the executor is {@link ForkingExecutor}, whereas {@link #testLogging()} used
     * {@link DrmaaSimpleModuleExecutor}.
     */
    @Test
    public void testForkingExecutorLogging() throws Exception {
        assert entityManagerFactory != null && scheduler != null && repository != null && linkerOptions != null;
        long executionId = 2;
        TestActorRef<DatabaseLoggingActor> actorRef = TestActorRef.create(
            actorSystem,
            Props.create(new DatabaseLoggingActor.Factory(entityManagerFactory, scheduler, EVICTION_DURATION)),
            "testForkingExecutorLogging database logger"
        );
        RuntimeAnnotatedExecutionTrace binarySumRootTrace = Linker.createAnnotatedExecutionTrace(
            ExecutionTrace.empty(),
            new MutableProxyModule().setDeclaration(BinarySum.class.getName()),
            Collections.<BareOverride>emptyList(),
            repository,
            linkerOptions
        );

        long baseTime = System.currentTimeMillis();
        sendTo(actorRef, BeginExecutionTraceEvent.of(executionId, baseTime, ExecutionTrace.empty()));
        ExecutionException executionException = new ExecutionException("Lorem ipsum");
        sendTo(
            actorRef,
            fabricatedForkingEndEvent(executionId, ExecutionTrace.empty(), baseTime, "foo bar", 1, executionException)
        );
        InterpreterException interpreterException
            = new InterpreterException(ExecutionTrace.empty(), "Fabricated exception.", executionException);
        sendTo(actorRef, FailedExecutionTraceEvent.of(executionId, baseTime + 200, ExecutionTrace.empty(),
            interpreterException));
        sendTo(actorRef, new StartExecutionEvent(executionId, binarySumRootTrace, "testForkingExecutorLogging"));
        sendTo(actorRef, new StopExecutionEvent(executionId));

        Entities.TablesContent tablesContent = Entities.getTablesContent(executionId, entityManagerFactory);
        Assert.assertEquals(
            tablesContent.getExecution(),
            new Execution()
                .setId(executionId)
                .setStartTime(new Date(baseTime))
                .setFinishTime(new Date(baseTime + END_EVENT_DELTA))
                .setKeyPrefix("testForkingExecutorLogging")
        );
        ExecutionFrame expectedExecutionFrame = new ExecutionFrame()
            .setExecution(tablesContent.getExecution())
            .setFrame("")
            .setStartTime(new Date(baseTime))
            .setFinishTime(new Date(baseTime + END_EVENT_DELTA))
            .setModuleKind(ExecutionFrame.ModuleKind.SIMPLE)
            .setSuccessful(false);
        Assert.assertEquals(tablesContent.getExecutionFrames(), Collections.singletonList(expectedExecutionFrame));
        Assert.assertEquals(
            tablesContent.getExecutionFrameProperties(),
            Collections.singletonList(fabricatedForkingProperties(expectedExecutionFrame, baseTime, "foo bar", 1))
        );
        Assert.assertEquals(
            tablesContent.getExecutionFrameErrors(),
            Collections.singletonList(
                new ExecutionFrameError()
                    .setExecutionFrame(expectedExecutionFrame)
                    .setErrorMessage(Throwables.executionTraceToString(interpreterException))
            )
        );
    }
}

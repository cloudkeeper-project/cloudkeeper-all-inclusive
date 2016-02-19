package com.svbio.workflow.service;

import com.svbio.workflow.api.WorkflowService;
import com.svbio.workflow.base.ConfigModule;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecycleManagerModule;
import com.svbio.workflow.bundles.core.Requirements;
import com.svbio.workflow.entities.Execution;
import com.svbio.workflow.entities.ExecutionFrame;
import com.svbio.workflow.entities.ExecutionFrameError;
import com.svbio.workflow.entities.ExecutionFrameProperties;
import com.svbio.workflow.entities.ProcessLauncherProperties;
import com.svbio.workflow.forkedexecutor.ForkedExecutor;
import com.svbio.workflow.runtimecontext.DaggerRuntimeContextComponent;
import com.svbio.workflow.runtimecontext.RuntimeContextComponent;
import com.svbio.workflow.util.SLF4JSessionLog;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.eclipse.persistence.config.TargetServer;
import org.eclipse.persistence.jpa.JpaEntityManagerFactory;
import org.eclipse.persistence.logging.SessionLog;
import org.h2.Driver;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import xyz.cloudkeeper.examples.modules.BinarySum;
import xyz.cloudkeeper.examples.modules.Decrease;
import xyz.cloudkeeper.examples.modules.DelayingModule;
import xyz.cloudkeeper.examples.modules.Fibonacci;
import xyz.cloudkeeper.examples.modules.GreaterOrEqual;
import xyz.cloudkeeper.examples.modules.ThrowingModule;
import xyz.cloudkeeper.interpreter.InterpreterException;
import xyz.cloudkeeper.maven.Bundles;
import xyz.cloudkeeper.maven.DummyAetherRepository;
import xyz.cloudkeeper.model.api.CancellationException;
import xyz.cloudkeeper.model.api.CloudKeeperEnvironment;
import xyz.cloudkeeper.model.api.WorkflowExecution;
import xyz.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import xyz.cloudkeeper.model.beans.element.annotation.MutableAnnotation;
import xyz.cloudkeeper.model.beans.element.module.MutableChildOutToParentOutConnection;
import xyz.cloudkeeper.model.beans.element.module.MutableCompositeModule;
import xyz.cloudkeeper.model.beans.element.module.MutableConnection;
import xyz.cloudkeeper.model.beans.element.module.MutableInPort;
import xyz.cloudkeeper.model.beans.element.module.MutableModule;
import xyz.cloudkeeper.model.beans.element.module.MutableOutPort;
import xyz.cloudkeeper.model.beans.element.module.MutableParentInToChildInConnection;
import xyz.cloudkeeper.model.beans.element.module.MutablePort;
import xyz.cloudkeeper.model.beans.element.module.MutableProxyModule;
import xyz.cloudkeeper.model.beans.execution.MutableElementTarget;
import xyz.cloudkeeper.model.beans.execution.MutableOverride;
import xyz.cloudkeeper.model.beans.execution.MutableOverrideTarget;
import xyz.cloudkeeper.model.beans.type.MutableDeclaredType;
import xyz.cloudkeeper.model.immutable.element.SimpleName;
import xyz.cloudkeeper.model.immutable.execution.ExecutionTrace;

import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ITWorkflowServiceComponent {
    private static final String SCHEMA = "ckcomponent_test";
    private static final int DEFAULT_MEMORY = 1;
    private static final int BINARY_SUM_MEMORY = 2;
    private static final int MEM_SCALE = 512;
    private static final int RUN_TIMEOUT_S = 60;
    private static final int KILL_PROCESS_TIMEOUT_MS = 1000;

    @Nullable private Path tempDir;
    @Nullable private LifecycleManager lifecycleManager;
    @Nullable private WorkflowService workflowService;
    @Nullable private EntityManagerFactory entityManagerFactory;

    @BeforeClass
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory(getClass().getSimpleName());
        String jdbcURL = "jdbc:h2:" + tempDir.resolve("database");
        Path stagingAreasBasePath = Files.createDirectory(tempDir.resolve("staging"));
        Path statusPath = Files.createDirectories(tempDir.resolve("status"));

        // Set up an Aether/Maven repository (the forked executor will use the Maven bundle loader)
        DummyAetherRepository aetherRepository = new DummyAetherRepository(tempDir);
        aetherRepository.installBundle("decrease", Arrays.asList(Decrease.class, Requirements.class));
        aetherRepository.installBundle("binarysum", Collections.singletonList(BinarySum.class), "decrease");
        aetherRepository.installBundle("fibonacci", Arrays.asList(GreaterOrEqual.class, Fibonacci.class), "binarysum");
        aetherRepository.installBundle("other", Arrays.asList(ThrowingModule.class, DelayingModule.class), "binarysum");

        // Generate configuration properties that point to the newly created Maven repository
        Map<String, Object> properties = new LinkedHashMap<>();
        // Let's use the Aether runtime-context factory for this test
        properties.put("com.svbio.workflow.loader", "aether");
        properties.put("com.svbio.workflow.maven.local",
            aetherRepository.getLocalRepository().getBasedir().toString());
        // The following property is needed for workflow-forked-executor
        properties.put("com.svbio.workflow.workspacebasepath", tempDir.toString());

        List<String> commandLine = new ArrayList<>();
        commandLine.add(Paths.get(System.getProperty("java.home")).resolve("bin").resolve("java").toString());
        commandLine.add("-enableassertions");
        commandLine.add("-classpath");
        commandLine.add("<classpath>");
        // The current content of 'properties' needs to be used as configuration in the forked Java process, too. The
        // easiest way is to define the properties as system properties.
        commandLine.addAll(
            properties.entrySet()
                .stream()
                .map(entry -> String.format("-D%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList())
        );
        commandLine.add("-Xmx%2$dm");
        commandLine.add(ForkedExecutor.class.getName());

        properties.put("com.svbio.workflow.forkingexecutor.commandline", commandLine);
        properties.put("com.svbio.workflow.forkingexecutor.memscale", MEM_SCALE);

        Map<String, String> javaPersistenceProperties = new LinkedHashMap<>();
        javaPersistenceProperties.put("javax.persistence.jdbc.driver", Driver.class.getName());
        javaPersistenceProperties.put("javax.persistence.jdbc.url", jdbcURL);
        javaPersistenceProperties.put("javax.persistence.jdbc.user", "");
        javaPersistenceProperties.put("javax.persistence.jdbc.password", "");
        javaPersistenceProperties.put("javax.persistence.schema-generation.database.action", "create");
        for (Map.Entry<String, String> entry: javaPersistenceProperties.entrySet()) {
            properties.put("com.svbio.workflow.database." + entry.getKey(), entry.getValue());
        }

        // EclipseLink properties
        javaPersistenceProperties.put(PersistenceUnitProperties.LOGGING_LEVEL, SessionLog.ALL_LABEL);
        javaPersistenceProperties.put(PersistenceUnitProperties.LOGGING_LOGGER, SLF4JSessionLog.class.getName());
        javaPersistenceProperties.put(PersistenceUnitProperties.TARGET_SERVER, TargetServer.None);

        properties.put("com.svbio.workflow.database.schema", SCHEMA);
        properties.put("com.svbio.workflow.filestaging.basepath", stagingAreasBasePath.toString());
        properties.put("com.svbio.workflow.filestatus.path", statusPath.toString());

        try (
            Connection connection = DriverManager.getConnection(jdbcURL);
            Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE SCHEMA " + SCHEMA);
        }

        Config config = ConfigFactory.parseMap(properties).withFallback(ConfigFactory.load());
        lifecycleManager = new LifecycleManager();
        RuntimeContextComponent runtimeContextComponent = DaggerRuntimeContextComponent.builder()
            .configModule(new ConfigModule(config))
            .lifecycleManagerModule(new LifecycleManagerModule(lifecycleManager))
            .build();
        WorkflowServiceComponent component = DaggerWorkflowServiceComponent.builder()
            .runtimeContextComponent(runtimeContextComponent)
            .build();
        workflowService = component.getWorkflowService();
        lifecycleManager.startAllServices();
        entityManagerFactory = Persistence.createEntityManagerFactory(
            Execution.class.getPackage().getName(), javaPersistenceProperties);
    }

    @AfterClass
    public void tearDown() throws Exception {
        assert lifecycleManager != null && tempDir != null;
        lifecycleManager.close();
        Files.walkFileTree(tempDir, RecursiveDeleteVisitor.getInstance());
    }

    private static MutableAnnotation getBinarySumAnnotation() {
        @Requirements(cpuCores = 2, memoryGB = BINARY_SUM_MEMORY)
        class Foo { }

        return MutableAnnotation.fromAnnotation(Foo.class.getAnnotation(Requirements.class));
    }

    private static List<MutableOverride> newOverridesList() {
        return Collections.singletonList(
            new MutableOverride()
                .setTargets(Collections.<MutableOverrideTarget<?>>singletonList(
                    new MutableElementTarget().setElement(BinarySum.class.getName())
                ))
                .setDeclaredAnnotations(Collections.singletonList(
                    getBinarySumAnnotation()
                ))
        );
    }

    @Test
    public void testSchema() {
        assert entityManagerFactory != null;
        String schema = entityManagerFactory.unwrap(JpaEntityManagerFactory.class).getServerSession()
            .getDescriptor(Execution.class).getTables().get(0).getTableQualifier();
        Assert.assertEquals(schema, SCHEMA);
    }

    @Test
    public void testFibonacci() throws Exception {
        assert workflowService != null && entityManagerFactory != null;

        URI bundleIdentifier = Bundles.bundleIdentifierFromMaven(
            DummyAetherRepository.GROUP_ID, "fibonacci", DummyAetherRepository.VERSION
        );
        String prefix = "testFibonacci";
        CloudKeeperEnvironment cloudKeeperEnvironment = workflowService.create(prefix, false);
        MutableProxyModule module = new MutableProxyModule().setDeclaration(Fibonacci.class.getName());
        WorkflowExecution workflowExecution = cloudKeeperEnvironment.newWorkflowExecutionBuilder(module)
            .setInputs(Collections.singletonMap(SimpleName.identifier("n"), 3))
            .setBundleIdentifiers(Collections.singletonList(bundleIdentifier))
            .setOverrides(newOverridesList())
            .start();
        Assert.assertEquals(workflowExecution.getOutput("result").get(RUN_TIMEOUT_S, TimeUnit.SECONDS), 2);
        long executionId = workflowExecution.getExecutionId().get(RUN_TIMEOUT_S, TimeUnit.SECONDS);
        workflowExecution.toCompletableFuture().get(RUN_TIMEOUT_S, TimeUnit.SECONDS);

        Entities.TablesContent tablesContent = Entities.getTablesContent(executionId, entityManagerFactory);
        List<ExecutionFrame> executionFrames = tablesContent.getExecutionFrames();
        Assert.assertEquals(executionFrames.size(), 14);
        ExecutionFrame last = executionFrames.get(13);
        Assert.assertNotNull(last.getSuccessful());
        Assert.assertTrue(last.getSuccessful());
        Assert.assertEquals(last.getModuleKind(), ExecutionFrame.ModuleKind.SIMPLE);
        Assert.assertEquals(last.getFrame(), ExecutionTrace.valueOf("/loop/1/sum").toString());
        Assert.assertNotNull(last.getFinishTime());
        Assert.assertNotNull(last.getStartTime());
        Assert.assertTrue(last.getFinishTime().getTime() > last.getStartTime().getTime());

        Assert.assertEquals(tablesContent.getExecutionFrameErrors(), Collections.<ExecutionFrame>emptyList());

        List<ExecutionFrameProperties<?>> executionFrameProperties = tablesContent.getExecutionFrameProperties();
        // Expected: Number of iterations times 3
        Assert.assertEquals(executionFrameProperties.size(), 6);
    }

    @Test
    public void testBinarySum() throws Exception {
        assert workflowService != null && entityManagerFactory != null;
        URI bundleIdentifier = Bundles.bundleIdentifierFromMaven(
            DummyAetherRepository.GROUP_ID, "binarysum", DummyAetherRepository.VERSION
        );
        String prefix = "testBinarySum";
        CloudKeeperEnvironment cloudKeeperEnvironment = workflowService.create(prefix, false);
        Map<SimpleName, Object> inputs = new LinkedHashMap<>(2);
        inputs.put(SimpleName.identifier("num1"), 10);
        inputs.put(SimpleName.identifier("num2"), 24);
        MutableProxyModule module = new MutableProxyModule().setDeclaration(BinarySum.class.getName());
        WorkflowExecution workflowExecution = cloudKeeperEnvironment.newWorkflowExecutionBuilder(module)
            .setInputs(inputs)
            .setBundleIdentifiers(Collections.singletonList(bundleIdentifier))
            .setOverrides(newOverridesList())
            .start();

        Assert.assertEquals(workflowExecution.getOutput("sum").get(RUN_TIMEOUT_S, TimeUnit.SECONDS), 34);
        long executionId = workflowExecution.getExecutionId().get(RUN_TIMEOUT_S, TimeUnit.SECONDS);
        workflowExecution.toCompletableFuture().get(RUN_TIMEOUT_S, TimeUnit.SECONDS);

        Entities.TablesContent tablesContent = Entities.getTablesContent(executionId, entityManagerFactory);
        List<ExecutionFrame> executionFrames = tablesContent.getExecutionFrames();
        Assert.assertEquals(executionFrames.size(), 1);
        ExecutionFrame last = executionFrames.get(0);
        Assert.assertNotNull(last.getSuccessful());
        Assert.assertTrue(last.getSuccessful());
        Assert.assertEquals(last.getModuleKind(), ExecutionFrame.ModuleKind.SIMPLE);
        Assert.assertEquals(last.getFrame(), ExecutionTrace.empty().toString());
        Assert.assertNotNull(last.getFinishTime());
        Assert.assertNotNull(last.getStartTime());
        Assert.assertTrue(last.getFinishTime().getTime() > last.getStartTime().getTime());

        @Nullable String commandLine
            = ((ProcessLauncherProperties<?>) tablesContent.getExecutionFrameProperties().get(0)).getCommandLine();
        Assert.assertNotNull(commandLine);
        Assert.assertTrue(commandLine.contains("-Xmx" + (MEM_SCALE * BINARY_SUM_MEMORY) + 'm'));

        Assert.assertEquals(tablesContent.getExecutionFrameErrors(), Collections.<ExecutionFrame>emptyList());
    }

    @Test
    public void testThrowingModule() throws Exception {
        assert workflowService != null && entityManagerFactory != null;
        URI bundleIdentifier = Bundles.bundleIdentifierFromMaven(
            DummyAetherRepository.GROUP_ID, "other", DummyAetherRepository.VERSION
        );
        String prefix = "testThrowingModule";
        CloudKeeperEnvironment cloudKeeperEnvironment = workflowService.create(prefix, false);
        MutableCompositeModule module = new MutableCompositeModule()
            .setDeclaredPorts(Arrays.<MutablePort<?>>asList(
                new MutableInPort()
                    .setSimpleName("in")
                    .setType(new MutableDeclaredType().setDeclaration(String.class.getName())),
                new MutableOutPort()
                    .setSimpleName("out")
                    .setType(new MutableDeclaredType().setDeclaration(Long.class.getName()))
            ))
            .setModules(Collections.<MutableModule<?>>singletonList(
                new MutableProxyModule()
                    .setSimpleName("child")
                    .setDeclaration(ThrowingModule.class.getName())
            ))
            .setConnections(Arrays.<MutableConnection<?>>asList(
                new MutableParentInToChildInConnection()
                    .setFromPort("in").setToModule("child").setToPort("string"),
                new MutableChildOutToParentOutConnection()
                    .setFromModule("child").setFromPort("size").setToPort("out")
            ));
        long startMillis = System.currentTimeMillis();
        WorkflowExecution workflowExecution = cloudKeeperEnvironment.newWorkflowExecutionBuilder(module)
            .setInputs(Collections.singletonMap(SimpleName.identifier("in"), "foo"))
            .setBundleIdentifiers(Collections.singletonList(bundleIdentifier))
            .setOverrides(newOverridesList())
            .start();
        long executionId = workflowExecution.getExecutionId().get(RUN_TIMEOUT_S, TimeUnit.SECONDS);
        try {
            workflowExecution.toCompletableFuture().get(RUN_TIMEOUT_S, TimeUnit.SECONDS);
            Assert.fail("Expected exception.");
        } catch (ExecutionException executionException) {
            InterpreterException interpreterException = (InterpreterException) executionException.getCause();
            Exception userException = (Exception) interpreterException.getCause();
            Assert.assertTrue(userException.getMessage().contains(ThrowingModule.ExpectedException.class.getName()));
        }

        Entities.TablesContent tablesContent = Entities.getTablesContent(executionId, entityManagerFactory);
        List<ExecutionFrame> executionFrames = tablesContent.getExecutionFrames();
        Assert.assertEquals(executionFrames.size(), 2);
        ExecutionFrame first = executionFrames.get(0);
        Assert.assertNotNull(first.getSuccessful());
        Assert.assertFalse(first.getSuccessful());
        Assert.assertEquals(first.getModuleKind(), ExecutionFrame.ModuleKind.COMPOSITE);
        Assert.assertEquals(first.getFrame(), ExecutionTrace.empty().toString());
        Assert.assertNotNull(first.getFinishTime());
        Assert.assertNotNull(first.getStartTime());
        Assert.assertTrue(first.getFinishTime().getTime() > first.getStartTime().getTime());

        ExecutionFrame second = executionFrames.get(1);
        Assert.assertNotNull(second.getSuccessful());
        Assert.assertFalse(second.getSuccessful());
        Assert.assertEquals(second.getModuleKind(), ExecutionFrame.ModuleKind.SIMPLE);
        Assert.assertEquals(second.getFrame(), ExecutionTrace.valueOf("/child").toString());
        Assert.assertNotNull(second.getFinishTime());
        Assert.assertNotNull(second.getStartTime());
        Assert.assertTrue(second.getFinishTime().getTime() > second.getStartTime().getTime());

        List<ExecutionFrameError> executionFrameErrors = tablesContent.getExecutionFrameErrors();
        Assert.assertEquals(executionFrameErrors.size(), 1);
        ExecutionFrameError executionFrameError = executionFrameErrors.get(0);
        Assert.assertSame(executionFrameError.getExecutionFrame(), second);
        Assert.assertNotNull(executionFrameError.getErrorMessage());
        Assert.assertTrue(
            executionFrameError.getErrorMessage().contains(ThrowingModule.ExpectedException.class.getName()));

        List<ExecutionFrameProperties<?>> executionFrameProperties = tablesContent.getExecutionFrameProperties();
        Assert.assertEquals(executionFrameProperties.size(), 1);
        ProcessLauncherProperties<?> processLauncherProperties
            = (ProcessLauncherProperties<?>) executionFrameProperties.get(0);
        Assert.assertSame(processLauncherProperties.getExecutionFrame(), second);
        Assert.assertNotNull(processLauncherProperties.getCommandLine());
        Assert.assertTrue(processLauncherProperties.getCommandLine().contains(ForkedExecutor.class.getName()));
        Assert.assertTrue(
            processLauncherProperties.getCommandLine().contains("-Xmx" + (MEM_SCALE * DEFAULT_MEMORY) + 'm')
        );
        Assert.assertNotNull(processLauncherProperties.getLauncherStartTime());
        Assert.assertNotNull(processLauncherProperties.getSimpleModuleStartTime());
        Assert.assertNotNull(processLauncherProperties.getLauncherFinishTime());
        Assert.assertTrue(
            startMillis <= processLauncherProperties.getLauncherStartTime().getTime()
            && processLauncherProperties.getLauncherStartTime().getTime()
                <= processLauncherProperties.getSimpleModuleStartTime().getTime()
            && processLauncherProperties.getSimpleModuleStartTime().getTime()
                <= processLauncherProperties.getLauncherFinishTime().getTime()
            && processLauncherProperties.getLauncherFinishTime().getTime() <= System.currentTimeMillis()
        );
    }

    @Test
    public void testDelayingModule() throws Exception {
        assert workflowService != null && entityManagerFactory != null;
        URI bundleIdentifier = Bundles.bundleIdentifierFromMaven(
            DummyAetherRepository.GROUP_ID, "other", DummyAetherRepository.VERSION
        );
        String prefix = "testDelayingModule";
        CloudKeeperEnvironment cloudKeeperEnvironment = workflowService.create(prefix, false);
        MutableProxyModule module = new MutableProxyModule().setDeclaration(DelayingModule.class.getName());
        WorkflowExecution workflowExecution = cloudKeeperEnvironment.newWorkflowExecutionBuilder(module)
            .setInputs(Collections.singletonMap(SimpleName.identifier("delaySeconds"), 60L))
            .setBundleIdentifiers(Collections.singletonList(bundleIdentifier))
            .setOverrides(newOverridesList())
            .start();
        long executionId = workflowExecution.getExecutionId().get(RUN_TIMEOUT_S, TimeUnit.SECONDS);

        // Wait a little for the active execution
        Thread.sleep(50);

        Assert.assertTrue(workflowExecution.isRunning());

        workflowExecution.cancel();

        // Wait one second so that the forked Java process is properly killed
        Thread.sleep(KILL_PROCESS_TIMEOUT_MS);
        Entities.TablesContent tablesContent = Entities.getTablesContent(executionId, entityManagerFactory);
        List<ExecutionFrame> executionFrames = tablesContent.getExecutionFrames();
        Assert.assertEquals(executionFrames.size(), 1);
        ExecutionFrame executionFrame = executionFrames.get(0);
        Assert.assertNotNull(executionFrame.getSuccessful());
        Assert.assertFalse(executionFrame.getSuccessful());
        Assert.assertEquals(executionFrame.getModuleKind(), ExecutionFrame.ModuleKind.SIMPLE);
        Assert.assertEquals(executionFrame.getFrame(), ExecutionTrace.empty().toString());
        Assert.assertNotNull(executionFrame.getFinishTime());
        Assert.assertNotNull(executionFrame.getStartTime());
        Assert.assertTrue(executionFrame.getFinishTime().getTime() > executionFrame.getStartTime().getTime());

        List<ExecutionFrameError> executionFrameErrors = tablesContent.getExecutionFrameErrors();
        Assert.assertEquals(executionFrameErrors.size(), 1);
        ExecutionFrameError executionFrameError = executionFrameErrors.get(0);
        Assert.assertSame(executionFrameError.getExecutionFrame(), executionFrame);
        Assert.assertNotNull(executionFrameError.getErrorMessage());
        Assert.assertTrue(
            executionFrameError.getErrorMessage().contains(CancellationException.class.getName()));

        // Since the workflow execution was cancelled, no simple-module execution result is available
        Assert.assertTrue(tablesContent.getExecutionFrameProperties().isEmpty());
    }
}

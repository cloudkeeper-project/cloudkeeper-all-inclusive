package com.svbio.workflow.samples.ckbundle;

import com.svbio.cloudkeeper.dsl.ModuleFactory;
import com.svbio.cloudkeeper.model.api.CloudKeeperEnvironment;
import com.svbio.cloudkeeper.model.api.WorkflowExecution;
import com.svbio.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import com.svbio.cloudkeeper.simple.SingleVMCloudKeeper;
import com.svbio.cloudkeeper.simple.WorkflowExecutions;
import com.svbio.workflow.api.WorkflowService;
import com.svbio.workflow.base.ConfigModule;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecycleManagerModule;
import com.svbio.workflow.runtimecontext.DaggerRuntimeContextComponent;
import com.svbio.workflow.runtimecontext.RuntimeContextComponent;
import com.svbio.workflow.service.DaggerWorkflowServiceComponent;
import com.svbio.workflow.service.WorkflowServiceComponent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ITPiModule {
    private static final int PRECISION = 10;
    private static final String EXPECTED = "3.141592653";

    @Nullable private Path tempDir;

    @BeforeClass
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory(getClass().getName());
    }

    @AfterClass
    private void tearDown() throws IOException, InterruptedException {
        assert tempDir != null;
        Files.walkFileTree(tempDir, RecursiveDeleteVisitor.getInstance());
    }

    /**
     * Verifies the {@link PiModule}, running it with the given CloudKeeper environment.
     *
     * <p>This method awaits the workflow execution using
     * {@link WorkflowExecutions#awaitFinish(WorkflowExecution, long, TimeUnit)}.
     */
    private static void runWithEnvironment(CloudKeeperEnvironment cloudKeeperEnvironment) throws Exception {
        PiModule module = ModuleFactory.getDefault().create(PiModule.class)
            .precision().fromValue(PRECISION);

        WorkflowExecution workflowExecution = module
            .newPreconfiguredWorkflowExecutionBuilder(cloudKeeperEnvironment)
            .start();

        WorkflowExecutions.awaitFinish(workflowExecution, 1, TimeUnit.MINUTES);
        Assert.assertEquals(
            WorkflowExecutions.getOutputValue(workflowExecution, module.digits(), 1, TimeUnit.SECONDS),
            EXPECTED
        );
    }

    /**
     * Verifies the {@link PiModule} module using the given configuration for a Lifecode CloudKeeper environment.
     */
    private static void runWithConfigMap(String prefix, Map<String, Object> configMap) throws Exception {
        Config config = ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load());
        try (LifecycleManager lifecycleManager = new LifecycleManager()) {
            RuntimeContextComponent runtimeContextComponent = DaggerRuntimeContextComponent.builder()
                .configModule(new ConfigModule(config))
                .lifecycleManagerModule(new LifecycleManagerModule(lifecycleManager))
                .build();
            WorkflowServiceComponent component = DaggerWorkflowServiceComponent.builder()
                .runtimeContextComponent(runtimeContextComponent)
                .build();
            CloudKeeperEnvironment cloudKeeperEnvironment
                = component.getWorkflowService().create(prefix, false);
            lifecycleManager.startAllServices();

            runWithEnvironment(cloudKeeperEnvironment);
        }
    }

    /**
     * Verifies the {@link PiModule} using a Lifcode CloudKeeper environment with a forking executor.
     *
     * <p>Every simple module is executed in a forked JVM. That is, debugging of simple modules is only possible by
     * attaching a debugger to the forked JVM.
     */
    @Test
    public void testForking() throws Exception {
        assert tempDir != null;
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("com.svbio.workflow.localexecutor.workspacebasepath", tempDir.toString());
        runWithConfigMap("forking", configMap);
    }

    /**
     * Verifies the {@link PiModule} using a Lifcode CloudKeeper environment with a DRMAA executor.
     *
     * <p>Every simple module is executed in a forked JVM that is scheduled using the DRMAA implementation. That is,
     * debugging of simple modules is only possible by attaching a debugger to the forked JVM.
     *
     * <p>With Maven, use the following command-line options (assuming that Grid Engine is the DRMAA implementation) to
     * ensure this test is run and not skipped:
     * <ul><li>
     *     {@code -Dmaven.test.additionalClasspath=${SGE_ROOT}/lib/drmaa.jar} ({@code SGE_ROOT} needs to be substituted,
     *     of course)
     * </li><li>
     *     {@code -DsystemPropertiesFile=/path/to/properties/file}, where the file contains the following lines (again
     *     after substitution):
     *     {@code
     *     java.library.path = ${SGE_ROOT}/lib/$(${SGE_ROOT}/util/arch)
     *     org.ggf.drmaa.SessionFactory = com.sun.grid.drmaa.SessionFactoryImpl
     *     }
     * </li></ul>
     */
    @Test
    public void testDrmaa() throws Exception {
        assert tempDir != null;
        if (System.getProperty("org.ggf.drmaa.SessionFactory") == null) {
            throw new SkipException(
                "Could not find system property org.ggf.drmaa.SessionFactory."
            );
        }

        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("com.svbio.workflow.localexecutor.workspacebasepath", tempDir.toString());
        configMap.put("com.svbio.workflow.executor", "drmaa");
        // Just for this test, don't translate resource requirements into Grid Engine native arguments
        configMap.put("com.svbio.workflow.drmaa.nativespec", "");
        configMap.put("com.svbio.workflow.drmaa.tmpdir", tempDir.toString());
        runWithConfigMap("drmaa", configMap);
    }

    /**
     * Verifies the {@link PiModule} module using a standard in-memory CloudKeeper environment.
     */
    @Test
    public void testInJVM() throws Exception {
        assert tempDir != null;
        SingleVMCloudKeeper cloudKeeper = new SingleVMCloudKeeper.Builder()
            .setWorkspaceBasePath(tempDir)
            .build();
        CloudKeeperEnvironment cloudKeeperEnvironment = cloudKeeper.newCloudKeeperEnvironmentBuilder()
            .setCleaningRequested(false)
            .build();
        runWithEnvironment(cloudKeeperEnvironment);
        cloudKeeper.shutdown().awaitTermination();
    }
}

package com.svbio.workflow.samples.embeddedck;

import com.svbio.workflow.base.ConfigModule;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecycleManagerModule;
import com.svbio.workflow.runtimecontext.DaggerRuntimeContextComponent;
import com.svbio.workflow.runtimecontext.RuntimeContextComponent;
import com.svbio.workflow.service.DaggerWorkflowServiceComponent;
import com.svbio.workflow.service.WorkflowServiceComponent;
import com.typesafe.config.Config;
import xyz.cloudkeeper.maven.Bundles;
import xyz.cloudkeeper.model.api.CloudKeeperEnvironment;
import xyz.cloudkeeper.model.api.WorkflowExecution;
import xyz.cloudkeeper.model.beans.element.module.MutableModule;
import xyz.cloudkeeper.model.beans.element.module.MutableProxyModule;
import xyz.cloudkeeper.model.immutable.element.SimpleName;
import xyz.cloudkeeper.model.immutable.element.Version;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class consists only of the static utility method {@link #computePi(Config)} for computing the digits of the
 * decimal representation of π.
 */
public final class PiComputer {
    private static final String PI_MODULE_CLASS_NAME = "com.svbio.workflow.samples.ckbundle.PiModule";
    private static final SimpleName PI_MODULE_INPUT_NAME = SimpleName.identifier("precision");
    private static final String PI_MODULE_OUTPUT_NAME = "digits";
    private static final String PI_MODULE_GROUP_ID = "com.svbio.workflow.samples";
    private static final String PI_MODULE_ARTIFACT_ID = "workflow-sample-ckbundle";
    private static final Version PI_MODULE_VERSION_ID;

    static {
        try (@Nullable InputStream inputStream = PiComputer.class.getResourceAsStream("version.properties")) {
            Objects.requireNonNull(inputStream);
            Properties properties = new Properties();
            properties.load(inputStream);
            PI_MODULE_VERSION_ID = Version.valueOf(properties.getProperty("project.version"));
        } catch (IOException exception) {
            throw new AssertionError("Could not load required resource.", exception);
        }
    }

    private PiComputer() {
        throw new AssertionError(String.format("No %s instances for you!", getClass().getName()));
    }

    private static String computePiWithEnvironment(CloudKeeperEnvironment cloudKeeperEnvironment, int precision)
            throws ExecutionException, TimeoutException, InterruptedException {
        MutableModule<?> module = new MutableProxyModule()
            .setDeclaration(PI_MODULE_CLASS_NAME);
        WorkflowExecution workflowExecution = cloudKeeperEnvironment
            .newWorkflowExecutionBuilder(module)
            .setInputs(Collections.singletonMap(PI_MODULE_INPUT_NAME, precision))
            .setBundleIdentifiers(Collections.singletonList(
                Bundles.bundleIdentifierFromMaven(PI_MODULE_GROUP_ID, PI_MODULE_ARTIFACT_ID, PI_MODULE_VERSION_ID))
            )
            .start();

        workflowExecution.toCompletableFuture().get(1, TimeUnit.MINUTES);
        return (String) workflowExecution.getOutput(PI_MODULE_OUTPUT_NAME).get(1, TimeUnit.SECONDS);
    }

    /**
     * Computes the decimal representation of π using the compute-pi CloudKeeper module.
     *
     * @param config configuration for the CloudKeeper environment
     * @return the decimal representation of π
     */
    public static String computePi(Config config) {
        int precision = config.getInt("com.svbio.pi.precision");
        try (LifecycleManager lifecycleManager = new LifecycleManager()) {
            RuntimeContextComponent runtimeContextComponent = DaggerRuntimeContextComponent.builder()
                .configModule(new ConfigModule(config))
                .lifecycleManagerModule(new LifecycleManagerModule(lifecycleManager))
                .build();
            WorkflowServiceComponent component = DaggerWorkflowServiceComponent.builder()
                .runtimeContextComponent(runtimeContextComponent)
                .build();
            CloudKeeperEnvironment cloudKeeperEnvironment
                = component.getWorkflowService().create(".", false);
            lifecycleManager.startAllServices();

            return computePiWithEnvironment(cloudKeeperEnvironment, precision);
        } catch (TimeoutException exception) {
            throw new IllegalStateException(
                "Timeout during compute-π module execution. The Maven remote repositories may not be available.",
                exception
            );
        } catch (ExecutionException | InterruptedException exception) {
            // Obviously, production code should try to recover or display a reasonable error message. In this example,
            // however, we indeed are in an unexpected, i.e., illegal, state.
            throw new IllegalStateException("Unexpected exception during compute-π module execution.", exception);
        }
    }
}

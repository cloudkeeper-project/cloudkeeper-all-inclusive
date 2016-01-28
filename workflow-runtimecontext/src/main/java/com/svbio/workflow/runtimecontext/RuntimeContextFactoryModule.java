package com.svbio.workflow.runtimecontext;

import com.svbio.cloudkeeper.model.api.RuntimeContextFactory;
import com.svbio.cloudkeeper.model.api.executor.ModuleConnectorProvider;
import com.svbio.cloudkeeper.model.api.staging.InstanceProvider;
import com.svbio.cloudkeeper.simple.LocalSimpleModuleExecutor;
import com.svbio.cloudkeeper.simple.PrefetchingModuleConnectorProvider;
import com.svbio.cloudkeeper.simple.SimpleInstanceProvider;
import com.svbio.workflow.base.LifecycleException;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import scala.concurrent.ExecutionContext;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Dagger module that provides a {@link RuntimeContextFactory}.
 */
@Module
final class RuntimeContextFactoryModule {
    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    static RuntimeContextFactory provideRuntimeContextFactory(RuntimeContextConfig configuration,
            @RuntimeContextFactoryQualifier Map<String, Provider<RuntimeContextFactory>> runtimeContextFactoryMap) {
        @Nullable Provider<RuntimeContextFactory> provider = runtimeContextFactoryMap.get(configuration.loader);
        if (provider == null) {
            throw new LifecycleException(String.format(
                "Unknown runtime-context factory '%s'. Available options: %s",
                configuration.loader, runtimeContextFactoryMap.keySet()
            ));
        }
        return provider.get();
    }

    @Provides
    @Singleton
    static InstanceProvider provideInstanceProvider(ExecutionContext executionContext,
            RuntimeContextFactory runtimeContextFactory) {
        return new SimpleInstanceProvider.Builder(executionContext)
            .setRuntimeContextFactory(runtimeContextFactory)
            .build();
    }

    @Provides
    @Singleton
    static ModuleConnectorProvider provideModuleConnectorProvider(ExecutorConfig config,
            ExecutionContext executionContext) {
        return new PrefetchingModuleConnectorProvider(config.workspaceBasePath, executionContext);
    }

    @Provides
    @Singleton
    static LocalSimpleModuleExecutor provideSimpleModuleExecutor(ExecutionContext executionContext,
            InstanceProvider instanceProvider, ModuleConnectorProvider moduleConnectorProvider) {
        return new LocalSimpleModuleExecutor.Builder(executionContext, moduleConnectorProvider)
            .setInstanceProvider(instanceProvider)
            .build();
    }

    @Singleton
    static final class RuntimeContextConfig {
        private final String loader;

        @Inject
        RuntimeContextConfig(Config config) {
            Config runtimeContextConfig = config.getConfig("com.svbio.workflow");
            loader = runtimeContextConfig.getString("loader");
        }
    }

    @Singleton
    static final class ExecutorConfig {
        private final Path workspaceBasePath;

        @Inject
        ExecutorConfig(Config config) {
            Config aetherConfig = config.getConfig("com.svbio.workflow.localexecutor");
            workspaceBasePath = Paths.get(aetherConfig.getString("workspacebasepath"));
        }
    }
}

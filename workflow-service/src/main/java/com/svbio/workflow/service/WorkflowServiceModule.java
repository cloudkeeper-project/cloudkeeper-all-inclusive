package com.svbio.workflow.service;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.ExecutionContexts;
import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.WorkflowService;
import com.svbio.workflow.base.LifecycleException;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecyclePhase;
import com.svbio.workflow.base.LifecyclePhaseListener;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import scala.concurrent.ExecutionContext;
import xyz.cloudkeeper.executors.CommandProvider;
import xyz.cloudkeeper.executors.ForkingExecutor;
import xyz.cloudkeeper.interpreter.AdministratorActorCreator;
import xyz.cloudkeeper.interpreter.EventSubscription;
import xyz.cloudkeeper.interpreter.ExecutorActorCreator;
import xyz.cloudkeeper.interpreter.InstanceProviderActorCreator;
import xyz.cloudkeeper.interpreter.MasterInterpreterActorCreator;
import xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor;
import xyz.cloudkeeper.model.api.staging.InstanceProvider;
import xyz.cloudkeeper.model.util.ImmutableList;
import xyz.cloudkeeper.simple.LocalSimpleModuleExecutor;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Dagger module that provides a {@link WorkflowService}.
 *
 * <p>This module requires that the following instances are provided by other modules:
 * <ul><li>
 *     {@link Config} containing configuration settings
 * </li><li>
 *     {@link LifecycleManager} for registering instances that have a lifecycle and require start-up and/or clean-up
 *     actions.
 * </li></ul>
 *
 * <p>Moreover, other modules may provide additional {@link StagingAreaService}, {@link SimpleModuleExecutor}, or
 * {@link StatusKeepingService} implementations as "plug-ins". To that end, the respective map-providers have to be
 * annotated with {@link StagingAreaServiceQualifier}, {@link SimpleModuleExecutorQualifier}, or
 * {@link StatusKeepingServiceQualifier}, respectively.
 *
 * <p>Other modules may also provide:
 * <ul><li>
 *     {@link EventSubscription} instances using set-providers annotated with
 *     {@link InterpreterEventsQualifier}. Any subscription will receive CloudKeeper interpreter events of type
 *     {@link xyz.cloudkeeper.interpreter.event.Event}.
 * </li><li>
 *     {@link ActorRef} instances using set-providers annotated with {@link ExecutionEventQualifier}. Any subscribed
 *     actor will receive events of type {@link ExecutionEvent}.
 * </li></ul>
 */
@Module(
    includes = DrmaaSimpleModuleExecutorModule.class
)
final class WorkflowServiceModule {
    private static final String ADMINISTRATOR_NAME = "administrator";
    private static final String MASTER_INTERPRETER_NAME = "master-interpreter";
    private static final String INSTANCE_PROVIDER_NAME = "instance-provider";
    private static final String EXECUTOR_NAME = "executor";

    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    @WorkflowServiceScope
    static ActorSystem provideActorSystem(LifecycleManager lifecycleManager) {
        final ActorSystem actorSystem = ActorSystem.create("CloudKeeper-Service");
        lifecycleManager.addLifecyclePhaseListener(
            new LifecyclePhaseListener("Akka Actor System", LifecyclePhase.STARTED) {
                @Override
                protected void onStop() {
                    actorSystem.terminate();
                }
            }
        );
        return actorSystem;
    }

    @Provides
    @Named(ADMINISTRATOR_NAME)
    @WorkflowServiceScope
    static ActorRef provideAdministratorActorRef(ActorSystem actorSystem) {
        return actorSystem.actorOf(Props.create(AdministratorActorCreator.getInstance()),
            ADMINISTRATOR_NAME);
    }

    @Provides
    @Named(MASTER_INTERPRETER_NAME)
    @WorkflowServiceScope
    static ActorRef provideMasterInterpreter(ActorSystem actorSystem,
            @FirstExecutionIdQualifier long firstExecutionId) {
        return actorSystem.actorOf(
            Props.create(new MasterInterpreterActorCreator(firstExecutionId)), MASTER_INTERPRETER_NAME);
    }

    @Provides
    @Named(INSTANCE_PROVIDER_NAME)
    @WorkflowServiceScope
    static ActorRef providerInstanceProviderActor(ActorSystem actorSystem, InstanceProvider instanceProvider) {
        return actorSystem.actorOf(
            Props.create(new InstanceProviderActorCreator(instanceProvider)),
            INSTANCE_PROVIDER_NAME
        );
    }

    @Provides
    @Named(EXECUTOR_NAME)
    @WorkflowServiceScope
    static ActorRef provideExecutorActorRef(ActorSystem actorSystem,
            SimpleModuleExecutor simpleModuleExecutor) {
        return actorSystem.actorOf(Props.create(new ExecutorActorCreator(simpleModuleExecutor)), EXECUTOR_NAME);
    }

    @Provides
    @LongRunningQualifier
    @WorkflowServiceScope
    static ScheduledExecutorService provideLongRunningExecutorService(LifecycleManager lifecycleManager) {
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        lifecycleManager.addLifecyclePhaseListener(
            new LifecyclePhaseListener(ScheduledExecutorService.class.getSimpleName(), LifecyclePhase.STARTED) {
                @Override
                protected void onStop() {
                    executorService.shutdownNow();
                }
            }
        );
        return executorService;
    }

    @Provides
    @LongRunningQualifier
    @WorkflowServiceScope
    static ExecutionContext provideLongRunningExecutionContext(
            @LongRunningQualifier ScheduledExecutorService executorService) {
        return ExecutionContexts.fromExecutorService(executorService);
    }

    @Provides
    @WorkflowServiceScope
    static RequirementsProvider providerRequirementsProvider(ServiceConfiguration serviceConfiguration) {
        return new RequirementsProvider(serviceConfiguration.defaultCpu, serviceConfiguration.defaultMemory);
    }

    @Provides
    @WorkflowServiceScope
    static CommandProvider provideCommandProvider(ForkingExecutorConfiguration executorConfiguration,
            RequirementsProvider requirementsProvider) {
        List<String> extraClassNames = executorConfiguration.extraClasses;
        List<Class<?>> extraClasses = new ArrayList<>(extraClassNames.size());
        ClassLoader classLoader = WorkflowServiceModule.class.getClassLoader();
        for (String extraClassName : extraClassNames) {
            try {
                extraClasses.add(Class.forName(extraClassName, false, classLoader));
            } catch (ClassNotFoundException exception) {
                throw new LifecycleException(String.format(
                    "Class %s is contained in the list of classes to be added to the classpath of the simple-module "
                        + "executor. However, this class could not be loaded.",
                    extraClassName
                ));
            }
        }

        return new CommandProviderImpl(executorConfiguration.commandline, extraClasses, requirementsProvider,
            executorConfiguration.memoryScalingFactor);
    }

    @Provides(type = Provides.Type.MAP)
    @StagingAreaServiceQualifier
    @StagingAreaServiceKey("file")
    @WorkflowServiceScope
    static StagingAreaService provideFileStagingAreaService(FileStagingAreaConfiguration fileConfiguration,
            ExecutionContext shortLivedExecutionContext) {
        Path basePath = fileConfiguration.basePath;

        for (Path directory: fileConfiguration.hardlinkEnabledPaths) {
            try {
                if (!Files.getFileStore(directory).equals(Files.getFileStore(basePath))) {
                    throw new LifecycleException(String.format(
                        "Tried to enable hard-link based copying for '%s', but path is not on the same file "
                        + "store as the staging-area base path '%s'.", directory, basePath
                    ));
                }
            } catch (IOException exception) {
                throw new LifecycleException(String.format(
                    "Exception while testing whether hard-link enabled path '%s' and staging-area base path '%s' are "
                    + "on the same file store.", directory, basePath
                ), exception);
            }
        }

        return new FileStagingAreaService(fileConfiguration.basePath, fileConfiguration.hardlinkEnabledPaths,
            shortLivedExecutionContext);
    }

    @Provides
    @WorkflowServiceScope
    static StagingAreaService provideStagingAreaService(ServiceConfiguration serviceConfiguration,
            @StagingAreaServiceQualifier Map<String, Provider<StagingAreaService>> stagingAreaServiceMap) {
        @Nullable Provider<StagingAreaService> provider = stagingAreaServiceMap.get(serviceConfiguration.staging);
        if (provider == null) {
            throw new LifecycleException(String.format(
                "Unknown staging-area service '%s'. Available options: %s",
                serviceConfiguration.staging, stagingAreaServiceMap.keySet()
            ));
        }
        return provider.get();
    }

    @Provides(type = Provides.Type.MAP)
    @SimpleModuleExecutorQualifier
    @SimpleModuleExecutorKey("forking")
    @WorkflowServiceScope
    static SimpleModuleExecutor newForkingSimpleModuleExecutor(
            CommandProvider commandProvider,
            ExecutionContext shortLivedExecutionContext,
            @LongRunningQualifier ExecutionContext longRunningExecutionContext,
            InstanceProvider instanceProvider) {
        return new ForkingExecutor.Builder(shortLivedExecutionContext, longRunningExecutionContext, commandProvider)
            .setInstanceProvider(instanceProvider)
            .build();
    }

    @Provides(type = Provides.Type.MAP)
    @SimpleModuleExecutorQualifier
    @SimpleModuleExecutorKey("local")
    @WorkflowServiceScope
    static SimpleModuleExecutor newLocalSimpleModuleExecutor(LocalSimpleModuleExecutor executor) {
        return executor;
    }

    @Provides
    static SimpleModuleExecutor provideSimpleModuleExecutor(ServiceConfiguration serviceConfiguration,
            @SimpleModuleExecutorQualifier Map<String, Provider<SimpleModuleExecutor>> executorMap) {
        @Nullable Provider<SimpleModuleExecutor> provider = executorMap.get(serviceConfiguration.executor);
        if (provider == null) {
            throw new LifecycleException(String.format(
                "Unknown simple-module executor '%s'. Available options: %s",
                serviceConfiguration.executor, executorMap.keySet()
            ));
        }
        return provider.get();
    }

    /**
     * Provides JAXB context for persisting {@link ExecutionStatus} instances.
     */
    @Provides
    @WorkflowServiceScope
    static JAXBContext provideJAXBContext() {
        try {
            return JAXBContext.newInstance(ExecutionStatus.class);
        } catch (JAXBException exception) {
            throw new LifecycleException("Failed to create JAXB context.", exception);
        }
    }

    @Provides(type = Provides.Type.MAP)
    @StatusKeepingServiceQualifier
    @StatusKeepingServiceKey("file")
    @WorkflowServiceScope
    static StatusKeepingService provideFileStatusKeepingService(FileStatusConfiguration statusConfiguration,
            ExecutionContext shortLivedExecutionContext, JAXBContext jaxbContext) {
        return new FileStatusKeepingService(statusConfiguration.path, shortLivedExecutionContext, jaxbContext);
    }

    @Provides(type = Provides.Type.MAP)
    @StatusKeepingServiceQualifier
    @StatusKeepingServiceKey("none")
    @WorkflowServiceScope
    static StatusKeepingService provideNoStatusKeepingService() {
        return new NoStatusKeepingService();
    }

    @Provides
    static StatusKeepingService provideStatusKeepingService(ServiceConfiguration serviceConfiguration,
            @StatusKeepingServiceQualifier Map<String, Provider<StatusKeepingService>> statusKeepingMap) {
        @Nullable Provider<StatusKeepingService> provider = statusKeepingMap.get(serviceConfiguration.statusKeeping);
        if (provider == null) {
            throw new LifecycleException(String.format(
                "Unknown status-keeping service '%s'. Available options: %s",
                serviceConfiguration.statusKeeping, statusKeepingMap.keySet()
            ));
        }
        return provider.get();
    }

    @Provides
    @WorkflowServiceScope
    static CloudKeeperEnvironmentFactory provideEnvironmentFactory(
            ExecutionContext executionContext,
            @Named(ADMINISTRATOR_NAME) ActorRef administrator,
            @Named(MASTER_INTERPRETER_NAME) ActorRef masterInterpreter,
            @Named(EXECUTOR_NAME) ActorRef executor,
            @Named(INSTANCE_PROVIDER_NAME) ActorRef instanceProviderActor,
            InstanceProvider instanceProvider,
            StagingAreaService stagingAreaService,
            @InterpreterEventsQualifier Set<EventSubscription> interpreterEventSubscriptions) {
        return new CloudKeeperEnvironmentFactoryImpl(executionContext, administrator, masterInterpreter, executor,
            instanceProviderActor, instanceProvider, stagingAreaService,
            ImmutableList.copyOf(interpreterEventSubscriptions));
    }

    @Provides
    @WorkflowServiceScope
    static WorkflowService provideWorkflowService(
            CloudKeeperEnvironmentFactory environmentFactory,
            @ExecutionEventQualifier Set<ActorRef> executionEventSubscribers,
            StatusKeepingService statusKeepingService,
            ExecutionContext executionContext) {
        return new WorkflowServiceImpl(environmentFactory, ImmutableList.copyOf(executionEventSubscribers),
            statusKeepingService, executionContext);
    }

    @WorkflowServiceScope
    static final class ForkingExecutorConfiguration {
        private final List<String> commandline;
        private final List<String> extraClasses;
        private final int memoryScalingFactor;

        @Inject
        ForkingExecutorConfiguration(Config config) {
            Config serviceConfig = config.getConfig("com.svbio.workflow.forkingexecutor");
            commandline
                = Collections.unmodifiableList(new ArrayList<>(serviceConfig.getStringList("commandline")));
            extraClasses
                = Collections.unmodifiableList(new ArrayList<>(serviceConfig.getStringList("extraclasses")));
            memoryScalingFactor = serviceConfig.getInt("memscale");
        }
    }

    @WorkflowServiceScope
    static final class ServiceConfiguration {
        private final String executor;
        private final int defaultCpu;
        private final int defaultMemory;
        private final String staging;
        private final String statusKeeping;

        @Inject
        ServiceConfiguration(Config config) {
            Config serviceConfig = config.getConfig("com.svbio.workflow");
            executor = serviceConfig.getString("executor");
            defaultCpu = serviceConfig.getInt("requirements.cpu");
            defaultMemory = serviceConfig.getInt("requirements.memory");
            staging = serviceConfig.getString("staging");
            statusKeeping = serviceConfig.getString("status");
        }
    }

    @WorkflowServiceScope
    static final class FileStagingAreaConfiguration {
        private final Path basePath;
        private final List<Path> hardlinkEnabledPaths;

        @Inject
        FileStagingAreaConfiguration(Config config) {
            Config fileStagingConfig = config.getConfig("com.svbio.workflow.filestaging");
            basePath = Paths.get(fileStagingConfig.getString("basepath"));
            List<Path> newHardlinkEnabledPaths = fileStagingConfig.getStringList("hardlink").stream()
                .map(string -> Paths.get(string))
                .collect(Collectors.toList());
            hardlinkEnabledPaths = Collections.unmodifiableList(newHardlinkEnabledPaths);
        }
    }

    @WorkflowServiceScope
    static class FileStatusConfiguration {
        private final Path path;

        @Inject
        FileStatusConfiguration(Config config) {
            Config serviceConfig = config.getConfig("com.svbio.workflow.filestatus");
            path = Paths.get(serviceConfig.getString("path"));
        }
    }
}

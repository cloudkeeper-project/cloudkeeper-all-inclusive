package com.svbio.workflow.service;

import akka.actor.ActorRef;
import scala.concurrent.ExecutionContext;
import xyz.cloudkeeper.interpreter.CloudKeeperEnvironmentBuilder;
import xyz.cloudkeeper.interpreter.EventSubscription;
import xyz.cloudkeeper.model.api.CloudKeeperEnvironment;
import xyz.cloudkeeper.model.api.staging.InstanceProvider;
import xyz.cloudkeeper.model.util.ImmutableList;

import java.util.List;

final class CloudKeeperEnvironmentFactoryImpl implements CloudKeeperEnvironmentFactory {
    private final ExecutionContext executionContext;
    private final ActorRef administrator;
    private final ActorRef masterInterpreter;
    private final ActorRef instanceProviderActor;
    private final ActorRef executor;
    private final InstanceProvider instanceProvider;
    private final StagingAreaService stagingAreaService;
    private final List<EventSubscription> interpreterEventSubscriptions;

    CloudKeeperEnvironmentFactoryImpl(ExecutionContext executionContext, ActorRef administrator,
            ActorRef masterInterpreter, ActorRef executor, ActorRef instanceProviderActor,
            InstanceProvider instanceProvider, StagingAreaService stagingAreaService,
            List<EventSubscription> interpreterEventSubscriptions) {
        this.executionContext = executionContext;
        this.administrator = administrator;
        this.masterInterpreter = masterInterpreter;
        this.instanceProviderActor = instanceProviderActor;
        this.executor = executor;
        this.instanceProvider = instanceProvider;
        this.stagingAreaService = stagingAreaService;
        this.interpreterEventSubscriptions = ImmutableList.copyOf(interpreterEventSubscriptions);
    }

    /**
     * Creates and returns a {@link CloudKeeperEnvironment}.
     *
     * @param prefix prefix for the staging area, the meaning of prefix depends on the configured staging-area service
     * @param cleaningRequested whether intermediate results should be removed from the staging area as soon as they are
     *     no longer needed
     * @return the new {@link CloudKeeperEnvironment}
     */
    @Override
    public CloudKeeperEnvironment create(String prefix, boolean cleaningRequested) {
        return new CloudKeeperEnvironmentBuilder(executionContext,
                administrator, masterInterpreter, executor, instanceProvider,
                stagingAreaService.provideInitialStagingAreaProvider(prefix))
            .setInstanceProviderActorPath(instanceProviderActor.path().toStringWithoutAddress())
            .setCleaningRequested(cleaningRequested)
            .setEventListeners(interpreterEventSubscriptions)
            .build();
    }
}

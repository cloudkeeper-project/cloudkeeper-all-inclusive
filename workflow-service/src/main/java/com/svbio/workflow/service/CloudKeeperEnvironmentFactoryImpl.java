package com.svbio.workflow.service;

import akka.actor.ActorRef;
import xyz.cloudkeeper.interpreter.CloudKeeperEnvironmentBuilder;
import xyz.cloudkeeper.interpreter.EventSubscription;
import xyz.cloudkeeper.model.api.CloudKeeperEnvironment;
import xyz.cloudkeeper.model.api.staging.InstanceProvider;
import xyz.cloudkeeper.model.util.ImmutableList;

import java.util.List;
import java.util.concurrent.Executor;

final class CloudKeeperEnvironmentFactoryImpl implements CloudKeeperEnvironmentFactory {
    private final Executor executor;
    private final ActorRef administratorActor;
    private final ActorRef masterInterpreterActor;
    private final ActorRef instanceProviderActor;
    private final ActorRef executorActor;
    private final InstanceProvider instanceProvider;
    private final StagingAreaService stagingAreaService;
    private final List<EventSubscription> interpreterEventSubscriptions;

    CloudKeeperEnvironmentFactoryImpl(Executor executor, ActorRef administratorActor,
            ActorRef masterInterpreterActor, ActorRef executorActor, ActorRef instanceProviderActor,
            InstanceProvider instanceProvider, StagingAreaService stagingAreaService,
            List<EventSubscription> interpreterEventSubscriptions) {
        this.executor = executor;
        this.administratorActor = administratorActor;
        this.masterInterpreterActor = masterInterpreterActor;
        this.instanceProviderActor = instanceProviderActor;
        this.executorActor = executorActor;
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
        return new CloudKeeperEnvironmentBuilder(executor,
                administratorActor, masterInterpreterActor, executorActor, instanceProvider,
                stagingAreaService.provideInitialStagingAreaProvider(prefix))
            .setInstanceProviderActorPath(instanceProviderActor.path().toStringWithoutAddress())
            .setCleaningRequested(cleaningRequested)
            .setEventListeners(interpreterEventSubscriptions)
            .build();
    }
}

package com.svbio.workflow.runtimecontext;

import dagger.Module;
import dagger.Provides;
import xyz.cloudkeeper.model.api.RuntimeContextFactory;
import xyz.cloudkeeper.simple.DSLRuntimeContextFactory;

import javax.inject.Singleton;
import java.util.concurrent.Executor;

/**
 * Dagger module that provides a {@link RuntimeContextFactory}.
 *
 * <p>Specifically, the provided {@link RuntimeContextFactory} instance will be of type
 * {@link DSLRuntimeContextFactory}.
 */
@Module
final class DSLRuntimeContextFactoryModule {
    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides(type = Provides.Type.MAP)
    @RuntimeContextFactoryQualifier
    @RuntimeContextFactoryKey("dsl")
    @Singleton
    static RuntimeContextFactory provideRuntimeContextFactory(Executor executor) {
        return new DSLRuntimeContextFactory.Builder(executor).build();
    }
}

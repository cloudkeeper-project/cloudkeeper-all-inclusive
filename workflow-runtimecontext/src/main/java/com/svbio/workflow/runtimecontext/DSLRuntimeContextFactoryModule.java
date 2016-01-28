package com.svbio.workflow.runtimecontext;

import com.svbio.cloudkeeper.model.api.RuntimeContextFactory;
import com.svbio.cloudkeeper.simple.DSLRuntimeContextFactory;
import dagger.Module;
import dagger.Provides;
import scala.concurrent.ExecutionContext;

import javax.inject.Singleton;

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
    static RuntimeContextFactory provideRuntimeContextFactory(ExecutionContext executionContext) {
        return new DSLRuntimeContextFactory.Builder(executionContext).build();
    }
}

package com.svbio.workflow.base;

import dagger.Module;
import dagger.Provides;

import java.util.Objects;

/**
 * Dagger module that provides the {@link LifecycleManager} specified at construction time.
 */
@Module
public final class LifecycleManagerModule {
    private final LifecycleManager lifecycleManager;

    /**
     * Constructor from lifecycle manager.
     *
     * @param lifecycleManager the lifecycle manager that this module will provide.
     */
    public LifecycleManagerModule(LifecycleManager lifecycleManager) {
        Objects.requireNonNull(lifecycleManager);
        this.lifecycleManager = lifecycleManager;
    }

    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    LifecycleManager provideLifecycleManager() {
        return lifecycleManager;
    }
}

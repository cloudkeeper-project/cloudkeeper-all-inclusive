package com.svbio.workflow.base;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;

import java.util.Objects;

/**
 * Dagger module that provides a {@link Config} instance.
 */
@Module
public final class ConfigModule {
    private final Config config;

    /**
     * Default constructor for a module that will provide the {@link Config} instance obtained from
     * {@link ConfigFactory#load()} (at the time this constructor was invoked).
     */
    public ConfigModule() {
        this(ConfigFactory.load());
    }

    /**
     * Constructor for a module that will provide the given {@link Config} instance.
     *
     * @param config {@link Config} instance to provide
     */
    public ConfigModule(Config config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    Config provideConfig() {
        return config;
    }
}

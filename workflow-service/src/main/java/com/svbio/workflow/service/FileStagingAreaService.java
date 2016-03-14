package com.svbio.workflow.service;

import xyz.cloudkeeper.filesystem.FileStagingArea;
import xyz.cloudkeeper.model.api.staging.StagingAreaProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link StagingAreaService} that provides a file-based staging area.
 */
final class FileStagingAreaService implements StagingAreaService {
    private final Path basePath;
    private final List<Path> hardlinkEnabledPaths;
    private final Executor executor;

    FileStagingAreaService(Path basePath, List<Path> hardlinkEnabledPaths, Executor executor) {
        Objects.requireNonNull(basePath);
        Objects.requireNonNull(hardlinkEnabledPaths);
        Objects.requireNonNull(executor);

        this.basePath = basePath;
        this.hardlinkEnabledPaths = hardlinkEnabledPaths;
        this.executor = executor;
    }

    @Override
    public StagingAreaProvider provideInitialStagingAreaProvider(String prefix) {
        return (runtimeContext, executionTrace, instanceProvider)
            -> new FileStagingArea.Builder(runtimeContext, executionTrace, basePath.resolve(prefix), executor)
                .setHardLinkEnabledPaths(hardlinkEnabledPaths)
                .build();
    }
}

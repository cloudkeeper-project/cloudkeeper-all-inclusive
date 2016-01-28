package com.svbio.workflow.service;

import com.svbio.cloudkeeper.filesystem.FileStagingArea;
import com.svbio.cloudkeeper.model.api.staging.StagingAreaProvider;
import scala.concurrent.ExecutionContext;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@link StagingAreaService} that provides a file-based staging area.
 */
final class FileStagingAreaService implements StagingAreaService {
    private final Path basePath;
    private final List<Path> hardlinkEnabledPaths;
    private final ExecutionContext executionContext;

    FileStagingAreaService(Path basePath, List<Path> hardlinkEnabledPaths, ExecutionContext executionContext) {
        Objects.requireNonNull(basePath);
        Objects.requireNonNull(hardlinkEnabledPaths);
        Objects.requireNonNull(executionContext);

        this.basePath = basePath;
        this.hardlinkEnabledPaths = hardlinkEnabledPaths;
        this.executionContext = executionContext;
    }

    @Override
    public StagingAreaProvider provideInitialStagingAreaProvider(String prefix) {
        return (runtimeContext, executionTrace, instanceProvider)
            -> new FileStagingArea.Builder(runtimeContext, executionTrace, basePath.resolve(prefix),
                    executionContext)
                .setHardLinkEnabledPaths(hardlinkEnabledPaths)
                .build();
    }
}

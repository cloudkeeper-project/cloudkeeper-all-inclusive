package com.svbio.workflow.service;

import com.svbio.workflow.bundles.core.Requirements;
import com.svbio.workflow.service.RequirementsProvider.ActualRequirements;
import xyz.cloudkeeper.drm.NativeSpecificationProvider;
import xyz.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;

import java.util.Objects;

/**
 * Implementation of the CloudKeeper {@link NativeSpecificationProvider} functional interface.
 *
 * <p>This class translates {@link Requirements} annotations on CloudKeeper simple modules into
 * DRMAA native arguments, by passing a two-parameter format string to {@link String#format(String, Object...)} with CPU
 * and memory requirements as arguments.
 */
final class NativeSpecificationProviderImpl implements NativeSpecificationProvider {
    private final String formatString;
    private final RequirementsProvider requirementsProvider;
    private final int memoryScalingFactor;

    NativeSpecificationProviderImpl(String formatString, RequirementsProvider requirementsProvider,
            int memoryScalingFactor) {
        Objects.requireNonNull(formatString);
        Objects.requireNonNull(requirementsProvider);
        if (memoryScalingFactor < 1) {
            throw new IllegalArgumentException(String.format(
                "Expected DRMAA memory-scaling factor >= 1, but got %d.", memoryScalingFactor
            ));
        }
        this.formatString = formatString;
        this.requirementsProvider = requirementsProvider;
        this.memoryScalingFactor = memoryScalingFactor;
    }

    @Override
    public String getNativeSpecification(RuntimeAnnotatedExecutionTrace executionTrace) {
        ActualRequirements requirements = requirementsProvider.getRequirements(executionTrace);
        return String.format(formatString, requirements.getCpu(), memoryScalingFactor * requirements.getMemory());
    }
}

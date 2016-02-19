package com.svbio.workflow.service;

import com.svbio.workflow.bundles.core.Requirements;
import xyz.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Provider of the actual requirements used for a simple-module execution.
 *
 * <p>If a module does not have a {@link Requirements} annotation, the default requirements. specified at construction
 * time of this provider, will be used.
 */
final class RequirementsProvider {
    private final ActualRequirements defaultRequirements;

    RequirementsProvider(int cpu, int memory) {
        defaultRequirements = new ActualRequirements(cpu, memory);
    }

    ActualRequirements getRequirements(RuntimeAnnotatedExecutionTrace runtimeAnnotatedExecutionTrace) {
        @Nullable Requirements requirements = runtimeAnnotatedExecutionTrace.getAnnotation(Requirements.class);
        return requirements == null
            ? defaultRequirements
            : new ActualRequirements(requirements.cpuCores(), requirements.memoryGB());
    }

    static final class ActualRequirements {
        private final int cpu;
        private final int memory;

        ActualRequirements(int cpu, int memory) {
            this.cpu = cpu;
            this.memory = memory;
        }

        @Override
        public boolean equals(@Nullable Object otherObject) {
            if (this == otherObject) {
                return true;
            } else if (otherObject == null || getClass() != otherObject.getClass()) {
                return false;
            }

            ActualRequirements other = (ActualRequirements) otherObject;
            return Objects.equals(cpu, other.cpu)
                && Objects.equals(memory, other.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cpu, memory);
        }

        public int getCpu() {
            return cpu;
        }

        public int getMemory() {
            return memory;
        }
    }
}

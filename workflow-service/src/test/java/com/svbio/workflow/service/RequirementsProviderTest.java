package com.svbio.workflow.service;

import com.svbio.workflow.bundles.core.Requirements;
import com.svbio.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import com.svbio.workflow.service.RequirementsProvider.ActualRequirements;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

public class RequirementsProviderTest {
    @Test
    public void getRequirementsNonNull() {
        RequirementsProvider requirementsProvider = new RequirementsProvider(3, 4);

        Requirements requirements = Mockito.mock(Requirements.class);
        when(requirements.cpuCores()).thenReturn(5);
        when(requirements.memoryGB()).thenReturn(6);

        RuntimeAnnotatedExecutionTrace executionTrace = Mockito.mock(RuntimeAnnotatedExecutionTrace.class);
        when(executionTrace.getAnnotation(Requirements.class)).thenReturn(requirements);

        Assert.assertEquals(
            requirementsProvider.getRequirements(executionTrace),
            new ActualRequirements(5, 6)
        );
    }

    @Test
    public void getRequirementsNull() {
        RequirementsProvider requirementsProvider = new RequirementsProvider(3, 4);

        RuntimeAnnotatedExecutionTrace executionTrace = Mockito.mock(RuntimeAnnotatedExecutionTrace.class);
        when(executionTrace.getAnnotation(Requirements.class)).thenReturn(null);

        Assert.assertEquals(
            requirementsProvider.getRequirements(executionTrace),
            new ActualRequirements(3, 4)
        );
    }
}

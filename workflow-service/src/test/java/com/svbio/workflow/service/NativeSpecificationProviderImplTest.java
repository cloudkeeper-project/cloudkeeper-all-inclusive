package com.svbio.workflow.service;

import com.svbio.workflow.bundles.core.Requirements;
import com.svbio.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

public class NativeSpecificationProviderImplTest {
    @Test
    public void testGetNativeSpecification() {
        NativeSpecificationProviderImpl nativeSpecificationProvider = new NativeSpecificationProviderImpl(
            "-l slots_free=%d,virtual_free=%dM",
            new RequirementsProvider(3, 4),
            256
        );

        Requirements requirements = Mockito.mock(Requirements.class);
        when(requirements.cpuCores()).thenReturn(5);
        when(requirements.memoryGB()).thenReturn(6);

        RuntimeAnnotatedExecutionTrace executionTrace = Mockito.mock(RuntimeAnnotatedExecutionTrace.class);
        when(executionTrace.getAnnotation(Requirements.class)).thenReturn(requirements);

        Assert.assertEquals(
            nativeSpecificationProvider.getNativeSpecification(executionTrace),
            "-l slots_free=" + 5 + ",virtual_free=" + (256 * 6) + 'M'
        );
    }
}

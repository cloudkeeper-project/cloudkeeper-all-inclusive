package com.svbio.workflow.samples.ckbundle;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PiComputerTest {
    @Test
    public void test() {
        Assert.assertEquals(
            PiComputer.computePi(100),
            "3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825342117067"
        );
    }
}

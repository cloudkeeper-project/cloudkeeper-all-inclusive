package com.svbio.workflow.samples.ckbundle;

import com.svbio.cloudkeeper.dsl.SimpleModule;
import com.svbio.cloudkeeper.dsl.SimpleModulePlugin;
import com.svbio.workflow.bundles.core.Requirements;

/**
 * Simple module that computes the digits of the decimal representation of π.
 */
@SimpleModulePlugin("Computes the digits of the decimal representation of π")
@Requirements(cpuCores = 1, memoryGB = 1)
public abstract class PiModule extends SimpleModule<PiModule> {
    public abstract InPort<Integer> precision();
    public abstract OutPort<String> digits();

    @Override
    public void run() {
        digits().set(PiComputer.computePi(precision().get()));
    }
}

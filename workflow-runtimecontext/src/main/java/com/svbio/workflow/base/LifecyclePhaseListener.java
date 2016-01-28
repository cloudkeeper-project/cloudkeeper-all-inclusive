package com.svbio.workflow.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Skeletal implementation of a listener to lifecycle events.
 *
 * <p>Many subservices started in this class needs to be explicitly started and/or stopped. Therefore, each such
 * subservice has a lifecycle consisting of three phases (see {@link LifecyclePhase}). It is impossible to revert to
 * a previous lifecycle phase. For instance, when a service has been stopped, it can never be started again. This
 * policy enforced in methods {@link #start()} and {@link #stop()}.
 */
public abstract class LifecyclePhaseListener {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String service;
    private LifecyclePhase phase;

    /**
     * Constructor.
     *
     * @param service description of the service
     * @param phase the initial lifecycle phase
     */
    protected LifecyclePhaseListener(String service, LifecyclePhase phase) {
        Objects.requireNonNull(service);
        Objects.requireNonNull(phase);
        this.service = service;
        this.phase = phase;
    }

    final void start() {
        if (phase == LifecyclePhase.STARTED) {
            return;
        } else if (phase == LifecyclePhase.STOPPED) {
            throw new IllegalStateException(String.format(
                "Tried to start %s even though lifecycle already reached state %s.", service, LifecyclePhase.STOPPED
            ));
        }

        try {
            onStart();
        } catch (Exception exception) {
            throw new LifecycleException(String.format(
                "Exception while starting %s.", service
            ), exception);
        }
        phase = LifecyclePhase.STARTED;
    }

    final void stop() {
        LifecyclePhase oldPhase = phase;
        phase = LifecyclePhase.STOPPED;
        if (oldPhase != LifecyclePhase.STARTED) {
            return;
        }

        try {
            onStop();
        } catch (Exception exception) {
            log.warn(String.format("Ignoring exception while stopping %s.", service), exception);
        }
    }

    protected void onStart() throws Exception { }
    protected void onStop() throws Exception { }
}

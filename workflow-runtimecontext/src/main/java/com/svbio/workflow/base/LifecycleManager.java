package com.svbio.workflow.base;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

/**
 * Lifecycle Manager that services can register with, so that they all are started and shutdown at the same time.
 *
 * <p>Non-trivial applications typically consist of multiple services that need to be created, started, and eventually
 * stopped. A lifecycle manager allows to register listeners to lifecycle-phase changes in order to keep the state of
 * the services in sync with the state of the lifecycle manager.
 *
 * <p>If possible, instances of this class should be used in try-with-resources statements. Closing a lifecycle manager
 * triggers {@link LifecyclePhaseListener#stop()} to be called on all listeners previously registered through
 * {@link #addLifecyclePhaseListener(LifecyclePhaseListener)}.
 */
public final class LifecycleManager implements AutoCloseable {
    private final List<LifecyclePhaseListener> hooks = new LinkedList<>();
    private LifecyclePhase phase = LifecyclePhase.INITIALIZED;

    /**
     * Adds a lifecycle-phase listener that is invoked for all lifecycle-phase transitions.
     *
     * <p>Lifecycle-phase transitions are caused by {@link #startAllServices()} or {@link #close()}.
     *
     * @param listener the lifecycle-phase listener
     */
    public void addLifecyclePhaseListener(LifecyclePhaseListener listener) {
        Objects.requireNonNull(listener);
        hooks.add(listener);
    }

    /**
     * Advances the lifecycle phase from {@link LifecyclePhase#INITIALIZED} to {@link LifecyclePhase#STARTED}.
     *
     * @throws LifecycleException if a service cannot be started
     */
    public void startAllServices() {
        if (phase != LifecyclePhase.INITIALIZED) {
            throw new IllegalStateException("Tried to start services more than once.");
        }

        phase = LifecyclePhase.STARTED;
        hooks.forEach(LifecyclePhaseListener::start);
    }

    /**
     * Advances the lifecycle phase from {@link LifecyclePhase#STARTED} to {@link LifecyclePhase#STOPPED}.
     *
     * <p>This method is idempotent. Listeners are informed in the <em>reverse</em> order of their registration through
     * {@link #addLifecyclePhaseListener(LifecyclePhaseListener)}.
     *
     * @throws LifecycleException if a service cannot be stopped
     */
    @Override
    public void close() {
        phase = LifecyclePhase.STOPPED;
        for (ListIterator<LifecyclePhaseListener> it = hooks.listIterator(hooks.size()); it.hasPrevious();) {
            it.previous().stop();
        }
    }
}

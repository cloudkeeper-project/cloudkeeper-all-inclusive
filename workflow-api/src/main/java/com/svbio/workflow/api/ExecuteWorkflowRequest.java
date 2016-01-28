package com.svbio.workflow.api;

import com.svbio.cloudkeeper.model.beans.element.module.MutableModule;
import com.svbio.cloudkeeper.model.beans.execution.MutableOverride;

import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Request to execute a CloudKeeper workflow.
 *
 * <p>Implementation note: The list properties in this class are allowed to be null, because omitting these properties
 * is reasonable. JAXB distinguishes between null and empty collection properties in that null properties are entirely
 * omitted from the XML (the previous design choice therefore allows for shorter XML).
 */
@XmlRootElement(name = "execute-workflow-request")
@XmlType(propOrder = { "module", "bundleIdentifiers", "overrides", "prefix", "cleaningRequested" })
public final class ExecuteWorkflowRequest implements Serializable {
    private static final long serialVersionUID = -8416721969279599586L;

    @Nullable private MutableModule<?> module;
    private final ArrayList<URI> bundleIdentifiers = new ArrayList<>();
    private final ArrayList<MutableOverride> overrides = new ArrayList<>();
    @Nullable private String prefix;
    private boolean cleaningRequested = true;

    /**
     * Constructor for instance with default properties.
     */
    public ExecuteWorkflowRequest() { }

    /**
     * Copy constructor.
     *
     * <p>The newly constructed instance is guaranteed to share no mutable state with the original instance (not even
     * transitively through multiple object references).
     *
     * @param original original instance that is to be copied
     */
    public ExecuteWorkflowRequest(ExecuteWorkflowRequest original) {
        module = MutableModule.copyOfModule(original.module);
        bundleIdentifiers.addAll(original.getBundleIdentifiers());
        overrides.addAll(original.getOverrides().stream().map(MutableOverride::copyOf).collect(Collectors.toList()));
        prefix = original.prefix;
        cleaningRequested = original.cleaningRequested;
    }

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        ExecuteWorkflowRequest other = (ExecuteWorkflowRequest) otherObject;
        return Objects.equals(module, other.module)
            && bundleIdentifiers.equals(other.bundleIdentifiers)
            && overrides.equals(other.overrides)
            && Objects.equals(prefix, other.prefix)
            && cleaningRequested == other.cleaningRequested;
    }

    @Override
    public int hashCode() {
        return Objects.hash(module, bundleIdentifiers, overrides, prefix, cleaningRequested);
    }

    @XmlElementRef
    @Nullable
    public MutableModule<?> getModule() {
        return module;
    }

    public ExecuteWorkflowRequest setModule(@Nullable MutableModule<?> module) {
        this.module = module;
        return this;
    }

    @XmlElementWrapper(name = "bundle-identifiers")
    @XmlElement(name = "bundle-identifier")
    public List<URI> getBundleIdentifiers() {
        return bundleIdentifiers;
    }

    public ExecuteWorkflowRequest setBundleIdentifiers(List<URI> bundleIdentifiers) {
        Objects.requireNonNull(bundleIdentifiers);
        List<URI> backup = new ArrayList<>(bundleIdentifiers);
        this.bundleIdentifiers.clear();
        this.bundleIdentifiers.addAll(backup);
        return this;
    }

    @XmlElementWrapper(name = "overrides")
    @XmlElement(name = "override")
    public List<MutableOverride> getOverrides() {
        return overrides;
    }

    public ExecuteWorkflowRequest setOverrides(List<MutableOverride> overrides) {
        Objects.requireNonNull(overrides);
        List<MutableOverride> backup = new ArrayList<>(overrides);
        this.overrides.clear();
        this.overrides.addAll(backup);
        return this;
    }

    /**
     * Returns the prefix for the staging area.
     *
     * <p>The configured value will be passed to {@link WorkflowService#create(String, boolean)}. Its meaning depends on
     * the configured staging-area service.
     *
     * @return this builder
     */
    @XmlElement
    @Nullable
    public String getPrefix() {
        return prefix;
    }

    public ExecuteWorkflowRequest setPrefix(@Nullable String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * Returns whether intermediate results should be removed from the staging area as soon as they are no longer
     * needed.
     *
     * <p>The configured value will be passed to will be passed to {@link WorkflowService#create(String, boolean)}.
     *
     * @return this builder
     */
    @XmlElement(name = "cleaning-requested")
    public boolean isCleaningRequested() {
        return cleaningRequested;
    }

    public ExecuteWorkflowRequest setCleaningRequested(boolean cleaningRequested) {
        this.cleaningRequested = cleaningRequested;
        return this;
    }
}

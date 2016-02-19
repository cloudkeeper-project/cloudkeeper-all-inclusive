package com.svbio.workflow.entities;

import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.util.Objects;

@Entity
@DiscriminatorValue("DRMAA")
public class DRMAAProperties extends ProcessLauncherProperties<DRMAAProperties> {
    @Nullable private String drmaaJobId;
    @Nullable private String nativeArguments;

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (!super.equals(otherObject)) {
            return false;
        }

        DRMAAProperties other = (DRMAAProperties) otherObject;
        return Objects.equals(drmaaJobId, other.drmaaJobId)
            && Objects.equals(nativeArguments, other.nativeArguments);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(drmaaJobId, nativeArguments);
    }

    @Nullable
    public String getDrmaaJobId() {
        return drmaaJobId;
    }

    public DRMAAProperties setDrmaaJobId(@Nullable String drmaaJobId) {
        this.drmaaJobId = drmaaJobId;
        return this;
    }

    /**
     * Returns the native arguments used for this DRMAA job submission.
     *
     * <p>The returned string is the value of property
     * {@link xyz.cloudkeeper.drm.DrmaaSimpleModuleExecutor#NATIVE_ARGUMENTS} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} returned by
     * {@link xyz.cloudkeeper.drm.DrmaaSimpleModuleExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}.
     */
    @Column(length = 256)
    @Nullable
    public String getNativeArguments() {
        return nativeArguments;
    }

    public DRMAAProperties setNativeArguments(@Nullable String nativeArguments) {
        this.nativeArguments = nativeArguments;
        return this;
    }
}

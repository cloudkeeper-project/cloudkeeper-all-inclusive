package com.svbio.workflow.service;

import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecyclePhase;
import com.svbio.workflow.base.LifecyclePhaseListener;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;
import xyz.cloudkeeper.drm.DrmaaSimpleModuleExecutor;
import xyz.cloudkeeper.drm.NativeSpecificationProvider;
import xyz.cloudkeeper.executors.CommandProvider;
import xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor;
import xyz.cloudkeeper.model.api.staging.InstanceProvider;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;

/**
 * Dagger module that ultimately provides a {@code "drmaa"}-annotated {@link SimpleModuleExecutor}.
 */
@Module
final class DrmaaSimpleModuleExecutorModule {
    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    static Session newDrmaaSession(LifecycleManager lifecycleManager) {
        final Session drmaaSession = SessionFactory.getFactory().getSession();
        lifecycleManager.addLifecyclePhaseListener(
            new LifecyclePhaseListener("DRMAA Session", LifecyclePhase.INITIALIZED) {
                @Override
                protected void onStart() throws DrmaaException {
                    drmaaSession.init(null);
                }

                @Override
                protected void onStop() throws DrmaaException {
                    drmaaSession.exit();
                }
            }
        );
        return drmaaSession;
    }

    @Provides(type = Provides.Type.MAP)
    @SimpleModuleExecutorQualifier
    @SimpleModuleExecutorKey("drmaa")
    @WorkflowServiceScope
    static SimpleModuleExecutor newDrmaaExecutor(LifecycleManager lifecycleManager,
            DrmaaConfiguration drmaaConfiguration, CommandProvider commandProvider,
            RequirementsProvider requirementsProvider, Executor shortLivedExecutor,
            @LongRunningQualifier ScheduledExecutorService longLivedExecutorService,
            InstanceProvider instanceProvider) {
        NativeSpecificationProvider nativeSpecificationProvider = new NativeSpecificationProviderImpl(
            drmaaConfiguration.nativeSpecification, requirementsProvider, drmaaConfiguration.memoryScalingFactor);
        Session drmaaSession = newDrmaaSession(lifecycleManager);
        return new DrmaaSimpleModuleExecutor.Builder(drmaaSession, drmaaConfiguration.jobIOBasePath, commandProvider,
                shortLivedExecutor, longLivedExecutorService)
            .setInstanceProvider(instanceProvider)
            .setNativeSpecificationProvider(nativeSpecificationProvider)
            .build();
    }

    static class DrmaaConfiguration {
        private final String nativeSpecification;
        private final int memoryScalingFactor;
        private final Path jobIOBasePath;

        @Inject
        DrmaaConfiguration(Config config) {
            Config drmaaConfig = config.getConfig("com.svbio.workflow.drmaa");
            nativeSpecification = drmaaConfig.getString("nativespec");
            memoryScalingFactor = drmaaConfig.getInt("memscale");
            jobIOBasePath = Paths.get(drmaaConfig.getString("tmpdir"));
        }
    }
}

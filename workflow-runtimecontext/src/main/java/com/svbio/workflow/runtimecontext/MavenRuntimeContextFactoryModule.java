package com.svbio.workflow.runtimecontext;

import com.svbio.workflow.base.LifecycleException;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecyclePhase;
import com.svbio.workflow.base.LifecyclePhaseListener;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.impl.SyncContextFactory;
import org.eclipse.aether.internal.impl.DefaultSyncContextFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import xyz.cloudkeeper.dsl.ModuleFactory;
import xyz.cloudkeeper.linker.ClassProvider;
import xyz.cloudkeeper.linker.ExecutableProvider;
import xyz.cloudkeeper.maven.FileLockSyncContextFactory;
import xyz.cloudkeeper.maven.MavenRuntimeContextFactory;
import xyz.cloudkeeper.model.api.Executable;
import xyz.cloudkeeper.model.api.RuntimeContextFactory;
import xyz.cloudkeeper.model.api.RuntimeStateProvisionException;
import xyz.cloudkeeper.model.immutable.element.Name;
import xyz.cloudkeeper.model.util.ImmutableList;
import xyz.cloudkeeper.simple.DSLExecutableProvider;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * Dagger module that provides a {@code "aether"}-annotated {@link RuntimeContextFactory}.
 *
 * <p>Specifically, the provided {@link RuntimeContextFactory} instance will be of type
 * {@link MavenRuntimeContextFactory}.
 */
@Module
public final class MavenRuntimeContextFactoryModule {
    /**
     * Type indicating that {@link org.eclipse.aether.internal.impl.SimpleLocalRepositoryManagerFactory} should be used
     * to manage a {@link LocalRepository}.
     */
    private static final String SIMPLE_REPOSITORY_MANAGER = "simple";

    @Nullable private final ClassLoader classLoader;

    /**
     * Default constructor for a module that will provide a {@link RuntimeContextFactory} that will create a new
     * {@link java.net.URLClassLoader} for each invocation of
     * {@link RuntimeContextFactory#newRuntimeContext(java.util.List)}.
     *
     * <p>The {@link MavenRuntimeContextFactory} will be built without configuring a class loader using
     * {@link MavenRuntimeContextFactory.Builder#setClassLoader(ClassLoader)}.
     */
    public MavenRuntimeContextFactoryModule() {
        classLoader = null;
    }

    /**
     * Constructor for a module that will provide a {@link RuntimeContextFactory} that will use the given class loader.
     *
     * <p>If a class cannot be found when a repository is linked (that is, during a call to
     * {@link RuntimeContextFactory#newRuntimeContext(java.util.List)}), this will be silently ignored. The rationale is
     * that a fixed class loader likely does not have access to all classes needed to execute a workflow, but it can
     * load the classes needed to set inputs and retrieve outputs.
     *
     * @param classLoader {@link ClassLoader} that will be passed to
     *     {@link MavenRuntimeContextFactory.Builder#setClassLoader(ClassLoader)} when the provided
     *     {@link MavenRuntimeContextFactory} is built
     */
    public MavenRuntimeContextFactoryModule(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader);
        this.classLoader = classLoader;
    }

    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    @Singleton
    static SyncContextFactory provideSyncContextFactory(LifecycleManager lifecycleManager,
            AetherConfig aetherConfiguration) {
        if (aetherConfiguration.offline) {
            return new DefaultSyncContextFactory();
        }

        Path lockFile = aetherConfiguration.localRepositoryPath.resolve(aetherConfiguration.lockFileName);
        FileLockSyncContextFactory syncContextFactory;
        try {
            Files.createDirectories(lockFile.getParent());
            syncContextFactory = new FileLockSyncContextFactory(lockFile);
        } catch (IOException exception) {
            throw new LifecycleException(String.format(
                "Exception while creating local-Aether-repository lock file at '%s'.", lockFile
            ), exception);
        }
        lifecycleManager.addLifecyclePhaseListener(
            new LifecyclePhaseListener(SyncContextFactory.class.getSimpleName(), LifecyclePhase.STARTED) {
                @Override
                protected void onStop() throws IOException {
                    syncContextFactory.close();
                }
            }
        );
        return syncContextFactory;
    }

    @Provides
    @Singleton
    static RepositorySystem provideRepositorySystem(SyncContextFactory syncContextFactory) {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.setServices(SyncContextFactory.class, syncContextFactory);
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        return locator.getService(RepositorySystem.class);
    }

    @Provides
    @Singleton
    static LocalRepository provideLocalAetherRepository(AetherConfig aetherConfiguration) {
        Path localRepositoryPath = aetherConfiguration.localRepositoryPath;
        try {
            Files.createDirectories(localRepositoryPath);
        } catch (IOException exception) {
            throw new LifecycleException(String.format(
                "Exception while creating local Aether repository at '%s'.", localRepositoryPath
            ), exception);
        }
        return new LocalRepository(localRepositoryPath.toFile(), SIMPLE_REPOSITORY_MANAGER);
    }

    @Provides
    @Singleton
    static RepositorySystemSession provideRepositorySystemSession(RepositorySystem repositorySystem,
            LocalRepository localRepository, AetherConfig aetherConfiguration) {
        DefaultRepositorySystemSession repositorySystemSession = MavenRepositorySystemUtils.newSession();
        repositorySystemSession.setLocalRepositoryManager(
            repositorySystem.newLocalRepositoryManager(repositorySystemSession, localRepository)
        );
        repositorySystemSession.setOffline(aetherConfiguration.offline);
        return repositorySystemSession;
    }

    /**
     * Implementation of {@link ClassProvider} that does not fail if a class cannot be found.
     */
    private static final class ClassProviderImpl implements ClassProvider {
        private final ClassLoader classLoader;

        private ClassProviderImpl(ClassLoader classLoader) {
            this.classLoader = classLoader;
        }

        @Override
        public Optional<Class<?>> provideClass(Name name) {
            try {
                return Optional.of(Class.forName(name.getBinaryName().toString(), true, classLoader));
            } catch (ClassNotFoundException ignored) {
                return Optional.empty();
            }
        }
    }

    /**
     * Implementation of {@link ExecutableProvider} that does not fail if a class cannot be found.
     */
    private static final class ExecutableProviderImpl implements ExecutableProvider {
        private final ModuleFactory moduleFactory;
        private final DSLExecutableProvider executableProvider;

        private ExecutableProviderImpl(ClassLoader classLoader) {
            moduleFactory = new ModuleFactory(classLoader);
            executableProvider = new DSLExecutableProvider(moduleFactory);
        }

        @Override
        public Optional<Executable> provideExecutable(Name name) throws RuntimeStateProvisionException {
            try {
                moduleFactory.loadClass(name);
                return executableProvider.provideExecutable(name);
            } catch (ClassNotFoundException ignored) {
                return Optional.empty();
            }
        }
    }

    @Provides(type = Provides.Type.MAP)
    @RuntimeContextFactoryQualifier
    @RuntimeContextFactoryKey("aether")
    @Singleton
    RuntimeContextFactory provideRuntimeContextFactory(Executor executor, RepositorySystem repositorySystem,
            RepositorySystemSession repositorySystemSession, AetherConfig aetherConfiguration) {
        MavenRuntimeContextFactory.Builder builder = new MavenRuntimeContextFactory.Builder(
                executor, repositorySystem, repositorySystemSession)
            .setClassLoader(classLoader)
            .setRemoteRepositories(aetherConfiguration.remoteRepositories);
        boolean ignoreClassNotFound = classLoader != null;
        if (ignoreClassNotFound) {
            builder
                .setClassProviderProvider(ClassProviderImpl::new)
                .setExecutableProviderProvider(ExecutableProviderImpl::new);
        } else {
            builder.setExecutableProviderProvider(
                actualClassLoader -> new DSLExecutableProvider(new ModuleFactory(actualClassLoader))
            );
        }
        return builder.build();
    }

    @Singleton
    static final class AetherConfig {
        private final Path localRepositoryPath;
        private final String lockFileName;
        private final boolean offline;
        private final ImmutableList<RemoteRepository> remoteRepositories;

        @Inject
        AetherConfig(Config config) {
            Config aetherConfig = config.getConfig("com.svbio.workflow.maven");
            localRepositoryPath = Paths.get(aetherConfig.getString("local"));
            lockFileName = aetherConfig.getString("lockfile");
            offline = aetherConfig.getBoolean("offline");

            Config remotes = aetherConfig.getConfig("remote");
            remoteRepositories = remotes.root().keySet().stream()
                .map(id -> new RemoteRepository.Builder(id, "default", remotes.getString(id)).build())
                .collect(ImmutableList.collector());
        }
    }
}

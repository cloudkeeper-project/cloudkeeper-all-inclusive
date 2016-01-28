package com.svbio.workflow.runtimecontext;

import com.svbio.cloudkeeper.maven.FileLockSyncContextFactory;
import com.svbio.cloudkeeper.model.LinkerException;
import com.svbio.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import com.svbio.workflow.runtimecontext.MavenRuntimeContextFactoryModule.AetherConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.aether.RepositoryException;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.SyncContext;
import org.eclipse.aether.repository.LocalRepository;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public class ITMavenRuntimeContextFactoryModule {
    private Path tempDir;

    @BeforeClass
    public void setup() throws IOException, LinkerException, RepositoryException {
        tempDir = Files.createTempDirectory(getClass().getName());
    }

    @AfterClass
    public void tearDown() throws IOException {
        Files.walkFileTree(tempDir, RecursiveDeleteVisitor.getInstance());
    }

    /**
     * Verifies that the {@link FileLockSyncContextFactory} is actually used.
     *
     * <p>This is non-obvious because of the strangely over-designed dependency-injection solution used by Aether.
     */
    @Test
    public void testSyncContextFactory() throws IOException {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("com.svbio.workflow.maven.local", tempDir.toString());

        Path lockFile = tempDir.resolve("lockFile");
        try (FileLockSyncContextFactory syncContextFactory = new FileLockSyncContextFactory(lockFile)) {
            Config config = ConfigFactory.parseMap(properties).withFallback(ConfigFactory.load());
            AetherConfig aetherConfig = new AetherConfig(config);
            RepositorySystem repositorySystem = MavenRuntimeContextFactoryModule.provideRepositorySystem(syncContextFactory);
            LocalRepository localRepository = MavenRuntimeContextFactoryModule.provideLocalAetherRepository(aetherConfig);
            RepositorySystemSession repositorySystemSession
                = MavenRuntimeContextFactoryModule.provideRepositorySystemSession(
                repositorySystem, localRepository, aetherConfig);
            SyncContext syncContext = repositorySystem.newSyncContext(repositorySystemSession, false);
            Assert.assertTrue(syncContext.getClass().getEnclosingClass().equals(FileLockSyncContextFactory.class));
        }
    }
}

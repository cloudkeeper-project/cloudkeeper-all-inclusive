package com.svbio.workflow.runtimecontext;

import com.svbio.workflow.base.ConfigModule;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecycleManagerModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.aether.RepositoryException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import xyz.cloudkeeper.dsl.Module;
import xyz.cloudkeeper.examples.modules.BinarySum;
import xyz.cloudkeeper.maven.Bundles;
import xyz.cloudkeeper.maven.DummyAetherRepository;
import xyz.cloudkeeper.model.LinkerException;
import xyz.cloudkeeper.model.api.RuntimeContext;
import xyz.cloudkeeper.model.api.RuntimeContextFactory;
import xyz.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import xyz.cloudkeeper.model.immutable.element.Name;
import xyz.cloudkeeper.model.runtime.element.module.RuntimeSimpleModuleDeclaration;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class ITRuntimeContextComponent {
    private Path tempDir;
    private DummyAetherRepository dummyAetherRepository;

    @BeforeClass
    public void setup() throws IOException, LinkerException, RepositoryException {
        tempDir = Files.createTempDirectory(getClass().getName());
        dummyAetherRepository = new DummyAetherRepository(tempDir);
        dummyAetherRepository.installBundle("binarysum", Collections.singletonList(BinarySum.class));
    }

    @AfterClass
    public void tearDown() throws IOException {
        Files.walkFileTree(tempDir, RecursiveDeleteVisitor.getInstance());
    }

    private static void testNewRuntimeContext(Config config, URI bundleIdentifier) throws Exception {
        try (LifecycleManager lifecycleManager = new LifecycleManager()) {
            ConfigModule configModule = new ConfigModule(config);
            LifecycleManagerModule lifecycleManagerModule = new LifecycleManagerModule(lifecycleManager);

            RuntimeContextComponent runtimeContextComponent = DaggerRuntimeContextComponent.builder()
                .configModule(configModule)
                .lifecycleManagerModule(lifecycleManagerModule)
                .build();
            RuntimeContextFactory runtimeContextFactory = runtimeContextComponent.getRuntimeContextFactory();

            try (
                RuntimeContext runtimeContext
                    = runtimeContextFactory.newRuntimeContext(Collections.singletonList(bundleIdentifier)).get()
            ) {
                @Nullable RuntimeSimpleModuleDeclaration moduleDeclaration = runtimeContext.getRepository().getElement(
                    RuntimeSimpleModuleDeclaration.class,
                    Name.qualifiedName(BinarySum.class.getName())
                );
                Assert.assertNotNull(moduleDeclaration);
            }
        }
    }

    @Test
    public void newRuntimeContextMaven() throws Exception {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("com.svbio.workflow.loader", "aether");
        properties.put("com.svbio.workflow.maven.local",
            dummyAetherRepository.getLocalRepository().getBasedir().toString());
        properties.put("com.svbio.workflow.maven.offline", "true");
        Config config = ConfigFactory.parseMap(properties).withFallback(ConfigFactory.load());
        URI bundleIdentifier = Bundles.bundleIdentifierFromMaven(
            DummyAetherRepository.GROUP_ID, "binarysum", DummyAetherRepository.VERSION);
        testNewRuntimeContext(config, bundleIdentifier);
    }

    @Test
    public void newRuntimeContextDSL() throws Exception {
        Config config = ConfigFactory.load();
        URI bundleIdentifier = new URI(Module.URI_SCHEME, BinarySum.class.getName(), null);
        testNewRuntimeContext(config, bundleIdentifier);
    }
}

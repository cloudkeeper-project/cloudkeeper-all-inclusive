package com.svbio.workflow.runtimecontext;

import com.svbio.cloudkeeper.dsl.Module;
import com.svbio.cloudkeeper.examples.modules.BinarySum;
import com.svbio.cloudkeeper.maven.Bundles;
import com.svbio.cloudkeeper.maven.DummyAetherRepository;
import com.svbio.cloudkeeper.model.LinkerException;
import com.svbio.cloudkeeper.model.api.RuntimeContext;
import com.svbio.cloudkeeper.model.api.RuntimeContextFactory;
import com.svbio.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import com.svbio.cloudkeeper.model.immutable.element.Name;
import com.svbio.cloudkeeper.model.runtime.element.module.RuntimeSimpleModuleDeclaration;
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
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
                RuntimeContext runtimeContext = Await.result(
                    runtimeContextFactory.newRuntimeContext(Collections.singletonList(bundleIdentifier)),
                    Duration.Inf()
                )
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

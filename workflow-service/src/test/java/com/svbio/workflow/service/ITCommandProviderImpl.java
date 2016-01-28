package com.svbio.workflow.service;

import com.svbio.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import org.slf4j.impl.SimpleLogger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ITCommandProviderImpl {
    @Nullable private Path tempDir;

    @BeforeClass
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory(getClass().getName());
    }

    @AfterClass
    public void tearDown() throws IOException {
        assert tempDir != null;
        Files.walkFileTree(tempDir, RecursiveDeleteVisitor.getInstance());
    }

    @Test
    public void getDefaultClasspath() throws IOException {
        assert tempDir != null;
        List<Class<?>> classes = new ArrayList<>();
        List<String> optionalArtifacts = new ArrayList<>();
        CommandProviderImpl.assembleClassesAndArtifacts(classes, optionalArtifacts);

        List<Class<?>> extraClasses = Collections.singletonList(SimpleLogger.class);
        Assert.assertEquals(
            extraClasses.stream()
                .map(CommandProviderImpl::pathOfClass)
                .map(Path::getFileName)
                .map(Path::toString)
                .filter(name -> !optionalArtifacts.stream().anyMatch(name::startsWith))
                .count(),
            0
        );

        classes.addAll(extraClasses);
        LinkedHashSet<String> allJars = classes.stream().map(CommandProviderImpl::pathOfClass)
            .map(Path::getFileName)
            .map(Path::toString)
            .collect(Collectors.toCollection(LinkedHashSet::new));
        // Pretend that all optional artifacts are listed in the JAR's manifest, with some arbitrary version number
        String classPath = Stream
            .concat(
                allJars.stream(),
                optionalArtifacts.stream().map(name -> name + "-1.0.0-SNAPSHOT")
            )
            .collect(Collectors.joining(" "));

        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        attributes.put(Attributes.Name.CLASS_PATH, classPath);
        Path jarFilePath = tempDir.resolve("dummy.jar");
        try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(jarFilePath), manifest)) {
            // Nothing to be done here. Just make static code analysis happy.
            Assert.assertNotNull(jarOutputStream);
        }

        // The default class path must match the classpath in the Manifest as follows: First, all artifacts that start
        // with the name of an optional artifact are removed. Then the code sources belonging to the extra classes are
        // added.
        String defaultClasspath = CommandProviderImpl.getDefaultClasspath(jarFilePath, extraClasses);

        Set<String> actualJars = Stream.of(defaultClasspath.split(File.pathSeparator))
            .map(Paths::get)
            .map(Path::getFileName)
            .map(Path::toString)
            .collect(Collectors.toSet());
        allJars.add("dummy.jar");
        Assert.assertEquals(actualJars, allJars);
    }
}

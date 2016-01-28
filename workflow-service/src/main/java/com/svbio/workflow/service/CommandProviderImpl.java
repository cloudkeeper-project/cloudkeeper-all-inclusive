package com.svbio.workflow.service;

import com.svbio.cloudkeeper.executors.CommandProvider;
import com.svbio.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import com.svbio.workflow.bundles.core.Requirements;
import com.svbio.workflow.forkedexecutor.ForkedExecutor;
import com.svbio.workflow.service.RequirementsProvider.ActualRequirements;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of the CloudKeeper {@link CommandProvider} functional interface.
 *
 * <p>This class translates {@link Requirements} annotations on CloudKeeper simple modules into
 * command lines.
 */
final class CommandProviderImpl implements CommandProvider {
    private static final String DEFAULT_CLASSPATH_PLACEHOLDER = "<classpath>";
    private static final String DEFAULT_PROPERTIES_PLACEHOLDER = "<props>";

    private final List<Element> command;
    private final RequirementsProvider requirementsProvider;
    private final int memoryScalingFactor;

    abstract static class Element {
        private final String string;

        Element(String string) {
            Objects.requireNonNull(string);
            this.string = string;
        }

        @Override
        public boolean equals(@Nullable Object otherObject) {
            return this == otherObject || (
                otherObject != null
                    && getClass() == otherObject.getClass()
                    && string.equals(((Element) otherObject).string)
            );
        }

        @Override
        public int hashCode() {
            return string.hashCode();
        }

        abstract String format(ActualRequirements requirements, int memoryScalingFactor);

        @Override
        public String toString() {
            return string;
        }
    }

    static final class FixedElement extends Element {
        FixedElement(String string) {
            super(string);
        }

        @Override
        String format(ActualRequirements requirements, int memoryScalingFactor) {
            return toString();
        }
    }

    static final class FormatElement extends Element {
        FormatElement(String string) {
            super(string);
        }

        @Override
        String format(ActualRequirements requirements, int memoryScalingFactor) {
            return String.format(toString(), requirements.getCpu(), memoryScalingFactor * requirements.getMemory());
        }
    }

    static Stream<Element> replacePlaceholders(String string, List<Class<?>> extraClasses) {
        if (DEFAULT_CLASSPATH_PLACEHOLDER.equals(string)) {
            return Stream.of(new FixedElement(getDefaultClasspath(extraClasses)));
        } else if (DEFAULT_PROPERTIES_PLACEHOLDER.equals(string)) {
            return getDefaultProperties(System.getProperties()).map(FixedElement::new);
        } else {
            return Stream.of(string).map(FormatElement::new);
        }
    }

    CommandProviderImpl(List<String> command, List<Class<?>> extraClasses, RequirementsProvider requirementsProvider,
            int memoryScalingFactor) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(extraClasses);
        Objects.requireNonNull(requirementsProvider);
        if (memoryScalingFactor < 1) {
            throw new IllegalArgumentException(String.format(
                "Expected JVM memory-scaling factor >= 1, but got %d.", memoryScalingFactor
            ));
        }

        this.command = command.stream()
            .flatMap(element -> replacePlaceholders(element, extraClasses))
            .collect(Collectors.toList());
        this.requirementsProvider = requirementsProvider;
        this.memoryScalingFactor = memoryScalingFactor;
    }

    @Override
    public List<String> getCommand(RuntimeAnnotatedExecutionTrace executionTrace) {
        ActualRequirements requirements = requirementsProvider.getRequirements(executionTrace);
        return command.stream()
            .map(element -> element.format(requirements, memoryScalingFactor))
            .collect(Collectors.toList());
    }

    /**
     * Utility method that parses the {@code executor-classes.properties} resource and adds entries to the two given
     * lists.
     *
     * <p>The assembled information is required to start {@link ForkedExecutor#main(String[])} in a separate JVM.
     * This method does <em>not</em> add the {@link Class} instance corresponding to {@link ForkedExecutor}.
     *
     * @param requiredClasses List that all classes will be added to that must be available on the classpath of the new
     *     JVM.
     * @param optionalArtifacts List that all artifacts will be added to that are not compile-time or runtime
     *     dependencies of this project (as specified in the {@code executor-classes.properties} resource).
     */
    static void assembleClassesAndArtifacts(List<Class<?>> requiredClasses, List<String> optionalArtifacts) {
        Properties properties;
        try (InputStream inputStream = CommandProviderImpl.class.getResourceAsStream("executor-classes.properties")) {
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException exception) {
            throw new IllegalStateException(String.format(
                "Could not load resource that lists the classes needed to start %s in a separate JVM.",
                ForkedExecutor.class
            ), exception);
        }

        ClassLoader classLoader = CommandProviderImpl.class.getClassLoader();
        Set<String> artifactNames = properties.stringPropertyNames();
        for (String artifactName: artifactNames) {
            String className = properties.getProperty(artifactName).trim();
            if (className.isEmpty()) {
                optionalArtifacts.add(artifactName);
            } else {
                try {
                    requiredClasses.add(Class.forName(className, false, classLoader));
                } catch (ClassNotFoundException exception) {
                    throw new IllegalStateException(String.format(
                        "Unexpected failure to load class %s, which is contained in a code source needed to start %s "
                            + "in a separate JVM.", className, ForkedExecutor.class
                    ), exception);
                }
            }
        }
    }

    /**
     * Returns the {@link Path} to the code source of the given class.
     */
    static Path pathOfClass(Class<?> clazz) {
         try {
            return Paths.get(clazz.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException("Unexpected exception!", exception);
        }
    }

    /**
     * Returns the default classpath for invoking {@link ForkedExecutor#main(String[])} in a separate JVM.
     *
     * <p>This method makes a best-effort attempt to assemble the classpath and fails if it cannot do so. The following
     * constraints need to be satisfied in order for this method to succeed:
     * <ul><li>
     *     If a security manager is installed, it must allow calling {@link Class#getProtectionDomain()}.
     * </li><li>
     *     The project providing {@link ForkedExecutor} must have been built against the same versions of shared
     *     dependencies as this project.
     * </li><li>
     *     If {@code codePath} is a JAR, it will be verified that the code source in the assembled classpath contains
     *     have the same names as those in the JAR manifest. Optional artifacts (see
     *     {@link #assembleClassesAndArtifacts(List, List)}) will not be included in this sanity check.
     * </li></ul>
     *
     * @param codePath Path to the code source providing {@link ForkedExecutor}. If this is a JAR, then it must
     *     contain a manifest with a classpath entry.
     * @param extraClasses List of names of classes that should be present on the class path in addition to the classes
     *     referenced by the {@code executor-classes.properties} resource. The given list instance is <em>not</em>
     *     modified.
     * @return the default command line
     * @throws IllegalStateException if a classpath cannot be computed because of any of the reasons listed above
     */
    static String getDefaultClasspath(Path codePath, List<Class<?>> extraClasses) {
        Objects.requireNonNull(codePath);
        Objects.requireNonNull(extraClasses);

        // Artifacts that the project providing ForkedExecutor depends on, but that are not dependencies of this
        // project. Example: The SLF4J implementation. Clients of the current project should not be forced to use
        // slf4j-simple, even though ForkedExecutor's project has a runtime dependency on it.
        List<Class<?>> requiredClasses = new ArrayList<>();
        List<String> optionalArtifacts = new ArrayList<>();
        assembleClassesAndArtifacts(requiredClasses, optionalArtifacts);
        Set<Path> requiredClasspaths = requiredClasses.stream()
            .map(CommandProviderImpl::pathOfClass)
            .collect(Collectors.toCollection(LinkedHashSet::new));

        // If codePath points to a jar, we will do an extra verification step. We verify that the Class-Path attribute
        // in the Manifest matches requiredJars.
        if (Files.isRegularFile(codePath)) {
            LinkedHashSet<String> requiredJars = requiredClasspaths.stream()
                .map(Path::getFileName)
                .map(Path::toString)
                .collect(Collectors.toCollection(LinkedHashSet::new));

            Set<String> manifestJars;
            try (JarFile jarFile = new JarFile(codePath.toFile())) {
                manifestJars = new LinkedHashSet<>(Arrays.asList(
                    jarFile.getManifest()
                        .getMainAttributes()
                        .getValue(Attributes.Name.CLASS_PATH)
                        .split(" ")
                ));
            } catch (IOException exception) {
                throw new IllegalStateException(String.format(
                    "Could not read manifest of JAR file %s.", codePath
                ), exception);
            }
            manifestJars.removeIf(manifestJar -> optionalArtifacts.stream().anyMatch(manifestJar::startsWith));

            if (!requiredJars.equals(manifestJars)) {
                Set<String> missing = new LinkedHashSet<>(manifestJars);
                missing.removeAll(requiredJars);
                Set<String> unnecessary = new LinkedHashSet<>(requiredJars);
                unnecessary.removeAll(manifestJars);
                throw new IllegalStateException(String.format(
                    "Internal inconsistency detected while assembling classpath for %s. The manifest of the JAR "
                        + "containing %1$s contains the following entries that are missing from the assembled "
                        + "classpath: %s. Conversely, the following entries appear in the assembled classpath, but are "
                        + "not part of the manifest: %s", ForkedExecutor.class, missing, unnecessary
                ));
            }
        }

        requiredClasspaths.add(codePath);
        requiredClasspaths.addAll(
            extraClasses.stream()
                .map(CommandProviderImpl::pathOfClass)
                .collect(Collectors.toList())
        );
        return requiredClasspaths.stream().map(Path::toString).collect(Collectors.joining(File.pathSeparator));
    }

    /**
     * This method is equivalent to calling {@link #getDefaultClasspath(Path, List)} with the result of
     * {@code Paths.get(ForkedExecutor.class.getProtectionDomain().getCodeSource().getLocation().toURI())} as
     * argument.
     */
    static String getDefaultClasspath(List<Class<?>> extraClasses) {
        return getDefaultClasspath(pathOfClass(ForkedExecutor.class), extraClasses);
    }

    /**
     * Returns a stream containing the default system properties for invoking {@link ForkedExecutor#main(String[])}
     * in a separate JVM.
     *
     * <p>The system properties are intended to build a command line and are thus returned in form {@code -Dkey=value}.
     */
    static Stream<String> getDefaultProperties(Properties properties) {
        return properties.stringPropertyNames()
            .stream()
            .filter(key -> "config.file".equals(key) || key.startsWith("com.svbio.workflow."))
            .map(key -> String.format("-D%s=%s", key, properties.getProperty(key)));
    }
}

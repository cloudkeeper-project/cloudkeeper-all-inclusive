package com.svbio.workflow.service;

import com.svbio.workflow.bundles.core.Requirements;
import org.mockito.Mockito;
import org.slf4j.impl.SimpleLogger;
import org.testng.Assert;
import org.testng.annotations.Test;
import xyz.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

public class CommandProviderImplTest {
    @Test
    public void getCommand() {
        CommandProviderImpl commandProvider = new CommandProviderImpl(
            Arrays.asList("java", "-Xmx%2$dm"), Collections.emptyList(), new RequirementsProvider(3, 4), 256);

        Requirements requirements = Mockito.mock(Requirements.class);
        when(requirements.cpuCores()).thenReturn(5);
        when(requirements.memoryGB()).thenReturn(6);

        RuntimeAnnotatedExecutionTrace executionTrace = Mockito.mock(RuntimeAnnotatedExecutionTrace.class);
        when(executionTrace.getAnnotation(Requirements.class)).thenReturn(requirements);

        Assert.assertEquals(
            commandProvider.getCommand(executionTrace),
            Arrays.asList("java", "-Xmx" + (256 * 6) + 'm')
        );
    }

    @Test
    public void getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty("config.file", "/path/to/file");
        properties.setProperty("com.svbio.workflow.foo", "bar");
        properties.setProperty("java.home", "/path/to/java");
        Set<String> arguments = CommandProviderImpl.getDefaultProperties(properties).collect(Collectors.toSet());
        Assert.assertEquals(
            arguments,
            new LinkedHashSet<>(Arrays.asList("-Dconfig.file=/path/to/file", "-Dcom.svbio.workflow.foo=bar"))
        );
    }

    @Test
    public void replacePlaceholders() {
        List<Class<?>> extraClasses = Collections.singletonList(SimpleLogger.class);
        String defaultClasspath = CommandProviderImpl.getDefaultClasspath(extraClasses);
        Assert.assertEquals(
            CommandProviderImpl.replacePlaceholders("<classpath>", extraClasses).collect(Collectors.toList()),
            Collections.singletonList(new CommandProviderImpl.FixedElement(defaultClasspath))
        );

        Assert.assertEquals(
            CommandProviderImpl.replacePlaceholders("<props>", extraClasses).collect(Collectors.toList()),
            Collections.emptyList()
        );

        Assert.assertEquals(
            CommandProviderImpl.replacePlaceholders("foo", extraClasses).collect(Collectors.toList()),
            Collections.singletonList(new CommandProviderImpl.FormatElement("foo"))
        );
    }
}

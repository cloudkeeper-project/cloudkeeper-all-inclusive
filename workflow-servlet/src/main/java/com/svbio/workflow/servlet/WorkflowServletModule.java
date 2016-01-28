package com.svbio.workflow.servlet;

import dagger.Module;
import dagger.Provides;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.moxy.json.MoxyJsonConfig;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.http.HttpServlet;
import java.util.Collections;

/**
 * Dagger module that provides an {@link HttpServlet}.
 */
@Module
final class WorkflowServletModule {
    @Override
    public String toString() {
        return String.format("Dagger module '%s'", getClass().getSimpleName());
    }

    @Provides
    static MoxyJsonConfig provideMoxyJsonConfig() {
        return new MoxyJsonConfig()
            .setIncludeRoot(false)
            .setMarshallerProperties(
                Collections.singletonMap(MarshallerProperties.JSON_WRAPPER_AS_ARRAY_NAME, (Object) true)
            );
    }

    @Provides
    static ResourceConfig provideResourceConfig(WorkflowServiceResolver workflowServiceResolver,
            MoxyJsonConfig moxyJsonConfig) {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true);

        // Exception mappers, resources, features
        resourceConfig.register(UnknownExecutionIdExceptionMapper.class);
        resourceConfig.register(MoxyJsonFeature.class);
        resourceConfig.register(WorkflowServiceResource.class);

        // Singletons (such as providers instantiated outside of JAX-RS)
        resourceConfig.register(workflowServiceResolver);
        resourceConfig.register(moxyJsonConfig.resolver());
        return resourceConfig;
    }

    @Provides
    static HttpServlet provideHttpServlet(ResourceConfig resourceConfig) {
        return new ServletContainer(resourceConfig);
    }
}

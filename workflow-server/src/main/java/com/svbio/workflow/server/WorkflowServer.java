package com.svbio.workflow.server;

import com.svbio.workflow.base.ConfigModule;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecycleManagerModule;
import com.svbio.workflow.base.LifecyclePhase;
import com.svbio.workflow.base.LifecyclePhaseListener;
import com.svbio.workflow.runtimecontext.DaggerRuntimeContextComponent;
import com.svbio.workflow.runtimecontext.MavenRuntimeContextFactoryModule;
import com.svbio.workflow.runtimecontext.RuntimeContextComponent;
import com.svbio.workflow.service.DaggerWorkflowServiceComponent;
import com.svbio.workflow.service.WorkflowServiceComponent;
import com.svbio.workflow.servlet.DaggerWorkflowServletComponent;
import com.svbio.workflow.servlet.WorkflowServletComponent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CloudKeeper Service main class.
 *
 * <p>This class provides the {@link #main} method, which wires together CloudKeeper, file/S3, database, and web
 * services.
 */
public final class WorkflowServer {
    private WorkflowServer() {
        throw new AssertionError(String.format("No %s instances for you!", getClass().getName()));
    }

    private static class TopLevelExceptionHandler implements Thread.UncaughtExceptionHandler {
        private final Logger log = LoggerFactory.getLogger(getClass());

        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            log.error(
                "Fatal error due to uncaught exception. CloudKeeper Service will terminate with exit code 1.",
                throwable
            );
            System.exit(1);
        }
    }

    private static boolean shouldEarlyExit(Config config) {
        String printConfigKey = "com.svbio.workflow.printconfig";
        if (config.hasPath(printConfigKey)) {
            String path = config.getString(printConfigKey);
            Config configToPrint = path.isEmpty()
                ? config
                : config.getConfig(path).atPath(path);
            System.out.println(configToPrint.root().render());
            return true;
        } else {
            return false;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.setDefaultUncaughtExceptionHandler(new TopLevelExceptionHandler());
        Config config = ConfigFactory.load();
        if (shouldEarlyExit(config)) {
            return;
        }
        try (LifecycleManager lifecycleManager = new LifecycleManager()) {
            RuntimeContextComponent runtimeContextComponent = DaggerRuntimeContextComponent.builder()
                .lifecycleManagerModule(new LifecycleManagerModule(lifecycleManager))
                .configModule(new ConfigModule(config))
                .mavenRuntimeContextFactoryModule(
                    new MavenRuntimeContextFactoryModule(ClassLoader.getSystemClassLoader())
                )
                .build();
            WorkflowServiceComponent workflowServiceComponent = DaggerWorkflowServiceComponent.builder()
                .runtimeContextComponent(runtimeContextComponent)
                .build();
            WorkflowServletComponent servletComponent = DaggerWorkflowServletComponent.builder()
                .workflowServiceComponent(workflowServiceComponent)
                .build();

            Config httpConfig = config.getConfig("com.svbio.workflow.http");
            int port = httpConfig.getInt("port");

            final Server server = new Server(port);
            ServletContextHandler handler = new ServletContextHandler();
            handler.setContextPath("/");
            handler.addServlet(new ServletHolder(servletComponent.getServlet()), "/api/*");
            server.setHandler(handler);

            lifecycleManager.addLifecyclePhaseListener(
                new LifecyclePhaseListener("Jetty Server", LifecyclePhase.INITIALIZED) {
                    @Override
                    protected void onStart() throws Exception {
                        server.start();
                    }

                    @Override
                    protected void onStop() throws Exception {
                        server.stop();
                    }
                }
            );

            // Everything is hooked up, so start it.
            lifecycleManager.startAllServices();

            // Finally wait for the Jetty server to exit.
            server.join();
        }
    }
}

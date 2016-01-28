package com.svbio.workflow.forkedexecutor;

import com.svbio.cloudkeeper.executors.ForkedExecutors;
import com.svbio.workflow.base.LifecycleManager;
import com.svbio.workflow.base.LifecycleManagerModule;
import com.svbio.workflow.runtimecontext.DaggerRuntimeContextComponent;
import com.svbio.workflow.runtimecontext.RuntimeContextComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Forked executor.
 *
 * <p>This class executes a simple module similar to
 * {@link com.svbio.cloudkeeper.model.api.executor.SimpleModuleExecutor}. However, this class is meant to be run in a
 * separate JVM. Therefore, its "interface" is different. Instead of implementing method
 * {@link com.svbio.cloudkeeper.model.api.executor.SimpleModuleExecutor#submit(com.svbio.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)},
 * this class uses
 * {@link ForkedExecutors#run(com.svbio.cloudkeeper.model.api.executor.SimpleModuleExecutor, java.io.InputStream, java.io.OutputStream)}
 * to read the {@link com.svbio.cloudkeeper.model.api.RuntimeStateProvider} instance from standard in, and it writes the
 * {@link com.svbio.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} to standard out when done.
 */
public final class ForkedExecutor {
    private ForkedExecutor() {
        throw new AssertionError(String.format("No %s instances for you!", getClass().getName()));
    }

    /**
     * Main method.
     *
     * @param args command-line arguments
     * @throws IOException if an I/O error occurs
     * @throws ClassNotFoundException if deserialization from standard-in fails because a class is not available on the
     *     classpath
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Thread.setDefaultUncaughtExceptionHandler(new TopLevelExceptionHandler());
        try (LifecycleManager lifecycleManager = new LifecycleManager()) {
            RuntimeContextComponent runtimeContextComponent = DaggerRuntimeContextComponent.builder()
                .lifecycleManagerModule(new LifecycleManagerModule(lifecycleManager))
                .build();

            lifecycleManager.startAllServices();
            ForkedExecutors.run(runtimeContextComponent.getLocalSimpleModuleExecutor(), System.in, System.out);
        }
    }

    private static class TopLevelExceptionHandler implements Thread.UncaughtExceptionHandler {
        /**
         * Constant copied from {@code sysexits.h}, which appeared somewhere after 4.3BSD.
         */
        public static final int EX_IOERR = 74;

        private final Logger log = LoggerFactory.getLogger(getClass());

        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            int statusCode;
            if (throwable instanceof IOException) {
                statusCode = EX_IOERR;
            } else {
                statusCode = 1;
            }
            log.error(String.format(
                "Uncaught exception encountered. Process will terminate with status code %d.", statusCode
            ), throwable);
            System.exit(statusCode);
        }
    }
}

package com.svbio.workflow.service;

import akka.dispatch.ExecutionContexts;
import com.svbio.workflow.api.ExecuteWorkflowRequest;
import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.UnknownExecutionIdException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.concurrent.ExecutionContext;
import xyz.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import xyz.cloudkeeper.model.beans.element.module.MutableProxyModule;

import javax.annotation.Nullable;
import javax.xml.bind.JAXBContext;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITFileStatusKeepingService {
    @Nullable private Path tempDir;
    @Nullable private ExecutorService executorService;
    @Nullable private ExecutionContext executionContext;

    @BeforeClass
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory(getClass().getSimpleName());
        executorService = Executors.newCachedThreadPool();
        executionContext = ExecutionContexts.fromExecutorService(executorService);
    }

    @AfterClass
    public void tearDown() throws IOException {
        assert executorService != null && tempDir != null;
        executorService.shutdownNow();
        Files.walkFileTree(tempDir, RecursiveDeleteVisitor.getInstance());
    }

    @Test
    public void persistLoad() throws Exception {
        assert executionContext != null && tempDir != null;
        FileStatusKeepingService statusKeepingService = new FileStatusKeepingService(tempDir, executionContext,
            JAXBContext.newInstance(ExecutionStatus.class));

        ExecutionStatus executionStatus = new ExecutionStatus()
            .setExecutionId(2)
            .setStatus(ExecutionStatus.Status.RUNNING)
            .setRequest(
                new ExecuteWorkflowRequest()
                    .setBundleIdentifiers(Collections.singletonList(URI.create("x-test:foo")))
                    .setCleaningRequested(true)
                    .setModule(
                        new MutableProxyModule()
                            .setDeclaration("foo.bar")
                    )
            )
            .setFailureDescription("foo");
        ExecutionStatus persistResult = statusKeepingService.persistExecutionStatus(executionStatus).get();
        Assert.assertSame(persistResult, executionStatus);

        ExecutionStatus loadedStatus = statusKeepingService.loadExecutionStatus(2).get();
        Assert.assertNotSame(loadedStatus, executionStatus);
        Assert.assertEquals(loadedStatus, executionStatus);

        try {
            statusKeepingService.loadExecutionStatus(123).get();
            Assert.fail();
        } catch (ExecutionException exception) {
            @Nullable Throwable cause = exception.getCause();
            Assert.assertTrue(cause instanceof UnknownExecutionIdException);
            Assert.assertTrue(cause.getMessage().contains("123"));
        }
    }
}

package com.svbio.workflow.service;

import akka.dispatch.Futures;
import akka.dispatch.OnComplete;
import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.UnknownExecutionIdException;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import javax.annotation.Nullable;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link StatusKeepingService} that stores the execution status in the file system.
 */
final class FileStatusKeepingService implements StatusKeepingService {
    private final Path executionStatusPath;
    private final ExecutionContext executionContext;
    private final JAXBContext jaxbContext;

    FileStatusKeepingService(Path executionStatusPath, ExecutionContext executionContext, JAXBContext jaxbContext) {
        Objects.requireNonNull(executionStatusPath);
        Objects.requireNonNull(executionContext);
        Objects.requireNonNull(jaxbContext);

        this.executionStatusPath = executionStatusPath;
        this.executionContext = executionContext;
        this.jaxbContext = jaxbContext;
    }

    private Path executionStatusPath(long executionId) {
        return executionStatusPath.resolve(executionId + ".xml");
    }

    @Override
    public CompletableFuture<ExecutionStatus> persistExecutionStatus(ExecutionStatus executionStatus) {
        // Careful not to close over mutable state that may be modified before future is completed
        ExecutionStatus copiedExecutionStatus = new ExecutionStatus(executionStatus);
        Future<ExecutionStatus> scalaFuture = Futures.future(() -> {
            long executionId = executionStatus.getExecutionId();
            try (OutputStream outputStream = Files.newOutputStream(executionStatusPath(executionId))) {
                Marshaller marshaller = jaxbContext.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
                marshaller.marshal(copiedExecutionStatus, outputStream);
            }
            return executionStatus;
        }, executionContext);
        return newCompletableFuture(scalaFuture);
    }

    @Override
    public CompletableFuture<ExecutionStatus> loadExecutionStatus(final long executionId) {
        Future<ExecutionStatus> scalaFuture = Futures.future(() -> {
            try (InputStream inputStream = Files.newInputStream(executionStatusPath(executionId))) {
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                return (ExecutionStatus) unmarshaller.unmarshal(inputStream);
            } catch (NoSuchFileException ignore) {
                throw new UnknownExecutionIdException(executionId);
            }
        }, executionContext);
        return newCompletableFuture(scalaFuture);
    }

    /**
     * Returns a new {@link CompletableFuture} representing the same asynchronous computation as the given Scala
     * {@link Future}.
     *
     * <p>In the future, this method may be removed in favor of a Java-only implementation.
     */
    private <T> CompletableFuture<T> newCompletableFuture(Future<T> scalaFuture) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        scalaFuture.onComplete(new OnComplete<T>() {
            @Override
            public void onComplete(@Nullable Throwable failure, @Nullable T executionStatus) {
                if (failure != null) {
                    completableFuture.completeExceptionally(failure);
                } else {
                    completableFuture.complete(executionStatus);
                }
            }
        }, executionContext);
        return completableFuture;
    }
}

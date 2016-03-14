package com.svbio.workflow.service;

import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.UnknownExecutionIdException;
import net.florianschoppmann.java.futures.Futures;

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
import java.util.concurrent.Executor;

/**
 * Implementation of {@link StatusKeepingService} that stores the execution status in the file system.
 */
final class FileStatusKeepingService implements StatusKeepingService {
    private final Path executionStatusPath;
    private final Executor executor;
    private final JAXBContext jaxbContext;

    FileStatusKeepingService(Path executionStatusPath, Executor executor, JAXBContext jaxbContext) {
        Objects.requireNonNull(executionStatusPath);
        Objects.requireNonNull(executor);
        Objects.requireNonNull(jaxbContext);

        this.executionStatusPath = executionStatusPath;
        this.executor = executor;
        this.jaxbContext = jaxbContext;
    }

    private Path executionStatusPath(long executionId) {
        return executionStatusPath.resolve(executionId + ".xml");
    }

    @Override
    public CompletableFuture<ExecutionStatus> persistExecutionStatus(ExecutionStatus executionStatus) {
        // Careful not to close over mutable state that may be modified before future is completed
        ExecutionStatus copiedExecutionStatus = new ExecutionStatus(executionStatus);
        return Futures.supplyAsync(() -> {
            long executionId = executionStatus.getExecutionId();
            try (OutputStream outputStream = Files.newOutputStream(executionStatusPath(executionId))) {
                Marshaller marshaller = jaxbContext.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
                marshaller.marshal(copiedExecutionStatus, outputStream);
            }
            return executionStatus;
        }, executor);
    }

    @Override
    public CompletableFuture<ExecutionStatus> loadExecutionStatus(final long executionId) {
        return Futures.supplyAsync(() -> {
            try (InputStream inputStream = Files.newInputStream(executionStatusPath(executionId))) {
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                return (ExecutionStatus) unmarshaller.unmarshal(inputStream);
            } catch (NoSuchFileException ignore) {
                throw new UnknownExecutionIdException(executionId);
            }
        }, executor);
    }
}

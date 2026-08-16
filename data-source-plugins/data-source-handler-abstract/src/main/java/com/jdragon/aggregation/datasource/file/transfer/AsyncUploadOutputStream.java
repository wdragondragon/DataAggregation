package com.jdragon.aggregation.datasource.file.transfer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncUploadOutputStream extends OutputStream {

    private static final AtomicInteger THREAD_SEQUENCE = new AtomicInteger();

    private final PipedInputStream input;
    private final PipedOutputStream output;
    private final ExecutorService executor;
    private final Future<?> upload;
    private boolean closed;

    public AsyncUploadOutputStream(int bufferSize, UploadOperation operation) throws IOException {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive");
        }
        input = new PipedInputStream(bufferSize);
        output = new PipedOutputStream(input);
        executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable,
                    "file-upload-stream-" + THREAD_SEQUENCE.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        upload = executor.submit(() -> {
            try (InputStream source = input) {
                operation.upload(source);
                return null;
            }
        });
    }

    @Override
    public void write(int value) throws IOException {
        ensureOpen();
        output.write(value);
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException {
        ensureOpen();
        output.write(buffer, offset, length);
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();
        output.flush();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            output.close();
            upload.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = new IOException("streaming upload was interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            failure = cause instanceof IOException ioException
                    ? ioException
                    : new IOException("streaming upload failed", cause);
        } catch (IOException e) {
            failure = e;
        } finally {
            executor.shutdownNow();
            try {
                input.close();
            } catch (IOException closeError) {
                if (failure != null) {
                    failure.addSuppressed(closeError);
                } else {
                    failure = closeError;
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("upload stream is closed");
        }
        if (upload.isDone()) {
            try {
                upload.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("streaming upload was interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw cause instanceof IOException ioException
                        ? ioException
                        : new IOException("streaming upload failed", cause);
            }
        }
    }

    @FunctionalInterface
    public interface UploadOperation {
        void upload(InputStream input) throws Exception;
    }
}

package com.jdragon.aggregation.pluginloader.runtime;

import com.jdragon.aggregation.pluginloader.JarLoader;
import com.jdragon.aggregation.pluginloader.JarLoaderCenter;
import com.jdragon.aggregation.pluginloader.type.IPluginType;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Pins plugin revisions for one operation or job instance and owns loader references.
 */
public final class PluginRuntimeSession implements AutoCloseable {
    /**
     * A runtime session is never implicitly inherited by a child thread.  Inheritable
     * thread locals are unsafe with executor workers: a worker created while a job is
     * bound retains that session for every later task, even after the job closes it.
     * Callers creating plugin threads must use {@link #bind(Runnable)} explicitly.
     */
    private static final ThreadLocal<java.util.Deque<PluginRuntimeSession>> CURRENT =
            new ThreadLocal<java.util.Deque<PluginRuntimeSession>>() {
                @Override
                protected java.util.Deque<PluginRuntimeSession> initialValue() {
                    return new java.util.ArrayDeque<PluginRuntimeSession>();
                }
            };

    private final Map<String, ResolvedPlugin> resolvedPlugins = new LinkedHashMap<String, ResolvedPlugin>();
    private final Map<String, JarLoaderCenter.LoaderLease> loaderLeases =
            new LinkedHashMap<String, JarLoaderCenter.LoaderLease>();
    private final Map<String, Object> runtimeObjects = new LinkedHashMap<String, Object>();
    private final AtomicReference<State> state = new AtomicReference<State>(State.OPEN);

    private PluginRuntimeSession() {
    }

    public static PluginRuntimeSession open() {
        PluginRuntimeSession session = new PluginRuntimeSession();
        CURRENT.get().push(session);
        return session;
    }

    /** Creates an owner session that is only bound while {@link #call(Supplier)} executes. */
    public static PluginRuntimeSession createDetached() {
        return new PluginRuntimeSession();
    }

    public static PluginRuntimeSession current() {
        Deque<PluginRuntimeSession> sessions = CURRENT.get();
        return sessions.isEmpty() ? null : sessions.peek();
    }

    public synchronized ResolvedPlugin resolve(IPluginType pluginType, String pluginName) {
        ensureOpen();
        String coordinate = pluginType.getName() + "/" + pluginName;
        ResolvedPlugin existing = resolvedPlugins.get(coordinate);
        if (existing != null) {
            return existing;
        }
        return PluginRuntimeResolvers.withResolvedPlugin(pluginType, pluginName, resolved -> {
            ensureOpen();
            acquire(resolved);
            resolvedPlugins.put(coordinate, resolved);
            return resolved;
        });
    }

    public synchronized JarLoader acquire(ResolvedPlugin resolvedPlugin) {
        ensureOpen();
        JarLoaderCenter.LoaderLease lease = loaderLeases.get(resolvedPlugin.getCoordinate());
        if (lease == null) {
            lease = JarLoaderCenter.acquire(resolvedPlugin);
            loaderLeases.put(resolvedPlugin.getCoordinate(), lease);
        }
        return lease.getJarLoader();
    }

    public synchronized Map<String, String> revisions() {
        Map<String, String> revisions = new LinkedHashMap<String, String>();
        for (Map.Entry<String, ResolvedPlugin> entry : resolvedPlugins.entrySet()) {
            revisions.put(entry.getKey(), entry.getValue().getIdentity());
        }
        return revisions;
    }

    public synchronized Object getRuntimeObject(String key) {
        return runtimeObjects.get(key);
    }

    public synchronized void putRuntimeObject(String key, Object value) {
        ensureOpen();
        runtimeObjects.put(key, value);
    }

    /** Binds this session for a synchronous operation and restores the previous binding. */
    public <T> T call(Supplier<T> task) {
        if (task == null) {
            throw new IllegalArgumentException("task must not be null");
        }
        ensureOpen();
        CURRENT.get().push(this);
        try {
            return task.get();
        } finally {
            unbindFromCurrentThread();
        }
    }

    /** Binds this session while a child task runs. */
    public Runnable bind(Runnable task) {
        return () -> {
            ensureOpen();
            CURRENT.get().push(this);
            try {
                task.run();
            } finally {
                unbindFromCurrentThread();
            }
        };
    }

    @Override
    public void close() {
        unbindFromCurrentThread();
        if (state.compareAndSet(State.OPEN, State.CLOSING)) {
            releaseLeases();
        }
    }

    /**
     * Unbinds immediately but keeps loaders alive until all supplied plugin threads terminate.
     */
    public void closeAfterThreads(Thread... threads) {
        unbindFromCurrentThread();
        List<Thread> alive = new ArrayList<Thread>();
        if (threads != null) {
            for (Thread thread : threads) {
                if (thread != null && thread.isAlive()) {
                    alive.add(thread);
                }
            }
        }
        if (!state.compareAndSet(State.OPEN, State.CLOSING)) {
            return;
        }
        if (alive.isEmpty()) {
            releaseLeases();
            return;
        }
        Thread reaper = new Thread(() -> {
            for (Thread thread : alive) {
                boolean interrupted = false;
                while (thread.isAlive()) {
                    try {
                        thread.join();
                    } catch (InterruptedException ex) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            releaseLeases();
        }, "DataAggregation-PluginRuntime-Reaper");
        reaper.setDaemon(true);
        reaper.start();
    }

    private void unbindFromCurrentThread() {
        Deque<PluginRuntimeSession> sessions = CURRENT.get();
        if (!sessions.isEmpty() && sessions.peek() == this) {
            sessions.pop();
        } else {
            sessions.remove(this);
        }
        if (sessions.isEmpty()) {
            CURRENT.remove();
        }
    }

    private synchronized void releaseLeases() {
        try {
            for (JarLoaderCenter.LoaderLease lease : loaderLeases.values()) {
                lease.close();
            }
        } finally {
            loaderLeases.clear();
            runtimeObjects.clear();
            state.set(State.CLOSED);
        }
    }

    public boolean isClosed() {
        return state.get() == State.CLOSED;
    }

    private void ensureOpen() {
        if (state.get() != State.OPEN) {
            throw new IllegalStateException("Plugin runtime session is closing or already closed");
        }
    }

    private enum State {
        OPEN,
        CLOSING,
        CLOSED
    }
}

package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.core.job.simple.ContinuousStream;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PipelinePluginRuntimeSessionTest {

    @Test
    public void pipelineExecutorBindsTheOwningSession() throws Exception {
        CountDownLatch invoked = new CountDownLatch(1);
        AtomicReference<PluginRuntimeSession> observed = new AtomicReference<PluginRuntimeSession>();
        Pipeline pipeline = new Pipeline(handler(invoked, observed));
        PluginRuntimeSession session = PluginRuntimeSession.open();
        try {
            pipeline.process();
            assertTrue(invoked.await(5, TimeUnit.SECONDS));
            assertSame(session, observed.get());
        } finally {
            pipeline.stop();
            session.close();
        }
    }

    @Test
    public void legacyPipelineRemainsUsableWithoutASession() throws Exception {
        CountDownLatch invoked = new CountDownLatch(1);
        AtomicReference<PluginRuntimeSession> observed = new AtomicReference<PluginRuntimeSession>();
        com.jdragon.aggregation.core.job.pipline.StreamHandler handler =
                new com.jdragon.aggregation.core.job.pipline.StreamHandler() {
                    @Override
                    public void process(java.util.concurrent.BlockingQueue<Message> queue) {
                        observed.set(PluginRuntimeSession.current());
                        invoked.countDown();
                    }
                };
        com.jdragon.aggregation.core.job.pipline.Pipeline pipeline =
                new com.jdragon.aggregation.core.job.pipline.Pipeline(List.of(handler));
        try {
            pipeline.start();
            assertTrue(invoked.await(5, TimeUnit.SECONDS));
            assertNull(observed.get());
        } finally {
            pipeline.stop();
        }
    }

    @Test
    public void asyncPipelineExecutorBindsTheOwningSession() throws Exception {
        CountDownLatch invoked = new CountDownLatch(1);
        AtomicReference<PluginRuntimeSession> observed = new AtomicReference<PluginRuntimeSession>();
        com.jdragon.aggregation.core.job.pipline.asyn.StreamHandler handler =
                new com.jdragon.aggregation.core.job.pipline.asyn.StreamHandler() {
                    @Override
                    public void process() {
                        observed.set(PluginRuntimeSession.current());
                        invoked.countDown();
                        end();
                    }
                };
        com.jdragon.aggregation.core.job.pipline.asyn.Pipeline pipeline =
                new com.jdragon.aggregation.core.job.pipline.asyn.Pipeline("runtime-session-test", handler);
        PluginRuntimeSession session = PluginRuntimeSession.open();
        try {
            pipeline.process();
            assertTrue(invoked.await(5, TimeUnit.SECONDS));
            assertSame(session, observed.get());
        } finally {
            pipeline.stop();
            session.close();
        }
    }

    @Test
    public void continuousStreamBindsTheOwningSessionToProducerAndConsumer() throws Exception {
        ContinuousStream stream = new ContinuousStream();
        CountDownLatch invoked = new CountDownLatch(2);
        AtomicReference<PluginRuntimeSession> producerSession = new AtomicReference<PluginRuntimeSession>();
        AtomicReference<PluginRuntimeSession> consumerSession = new AtomicReference<PluginRuntimeSession>();
        AtomicInteger supplied = new AtomicInteger();
        stream.registerTransformer(message -> {
            producerSession.set(PluginRuntimeSession.current());
            invoked.countDown();
            return message;
        });
        stream.registerConsumer(message -> {
            consumerSession.set(PluginRuntimeSession.current());
            invoked.countDown();
        });
        PluginRuntimeSession session = PluginRuntimeSession.open();
        try {
            stream.startConsumer();
            stream.startProducer(() -> supplied.getAndIncrement() == 0 ? "record" : stopProducer());
            assertTrue(invoked.await(5, TimeUnit.SECONDS));
            assertSame(session, producerSession.get());
            assertSame(session, consumerSession.get());
        } finally {
            stream.stop();
            session.close();
        }
    }

    private StreamHandler handler(CountDownLatch invoked,
                                  AtomicReference<PluginRuntimeSession> observed) {
        return new StreamHandler() {
            @Override
            public void process() {
                observed.set(PluginRuntimeSession.current());
                invoked.countDown();
            }
        };
    }

    private String stopProducer() {
        throw new IllegalStateException("test producer completed");
    }
}

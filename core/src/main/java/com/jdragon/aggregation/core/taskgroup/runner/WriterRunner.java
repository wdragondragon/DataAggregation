package com.jdragon.aggregation.core.taskgroup.runner;

import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterRunner extends AbstractRunner implements Runnable {

    private static final Logger LOG = LoggerFactory
            .getLogger(WriterRunner.class);

    public WriterRunner(AbstractJobPlugin abstractJobPlugin) {
        super(abstractJobPlugin);
    }

    private RecordReceiver recordReceiver;

    public void setRecordReceiver(RecordReceiver receiver) {
        this.recordReceiver = receiver;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Writer.Job jobWriter = (Writer.Job) this.getPlugin();
        try {
            LOG.info("job writer init start");
            jobWriter.init();
            LOG.info("job writer init end");

            LOG.info("job writer prepare start");
            jobWriter.prepare();
            LOG.info("job writer prepare end");

            LOG.info("job writer startWriter");
            jobWriter.startWrite(recordReceiver);
            LOG.info("job writer endWriter");

            LOG.info("job writer post start");
            jobWriter.post();
            LOG.info("job writer post end");

            super.markSuccess();
        } catch (Throwable e) {
            LOG.error("Writer Runner Received Exceptions:", e);
            super.markFail(e);
        } finally {
            super.destroy();
        }
    }
}

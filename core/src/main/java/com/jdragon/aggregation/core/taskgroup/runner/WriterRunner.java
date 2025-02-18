package com.jdragon.aggregation.core.taskgroup.runner;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Setter
public class WriterRunner extends AbstractRunner {

    private static final Logger LOG = LoggerFactory
            .getLogger(WriterRunner.class);

    private RecordReceiver recordReceiver;

    public WriterRunner(AbstractJobPlugin abstractJobPlugin) {
        super(abstractJobPlugin);
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
            LOG.info("job writer prepare start");
            jobWriter.prepare();
            LOG.info("job writer prepare end");

            LOG.info("job writer startWriter");
            try {
                jobWriter.startWrite(recordReceiver);
            } catch (AggregationException e) {
                if (!(e.getCause() instanceof InterruptedException)) {
                    throw e;
                }
            }
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

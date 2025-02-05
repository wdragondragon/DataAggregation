package com.jdragon.aggregation.core.taskgroup.runner;

import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReaderRunner extends AbstractRunner implements Runnable {

    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderRunner.class);

    private RecordSender recordSender;

    public ReaderRunner(AbstractJobPlugin abstractJobPlugin) {
        super(abstractJobPlugin);
    }

    public void setRecordSender(RecordSender recordSender) {
        this.recordSender = recordSender;
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
        Reader.Job jobReader = (Reader.Job) this.getPlugin();
        try {
            LOG.info("job reader init start");
            jobReader.init();
            LOG.info("job reader init end");

            LOG.info("job reader prepare start");
            jobReader.prepare();
            LOG.info("job reader prepare end");

            LOG.info("job reader startRead");
            jobReader.startRead(recordSender);
            LOG.info("job reader endRead");
            recordSender.terminate();

            LOG.info("job reader post start");
            jobReader.post();
            LOG.info("job reader post end");
        } catch (Throwable e) {
            LOG.error("Writer Runner Received Exceptions:", e);
            super.markFail(e);
        } finally {
            super.destroy();
        }
    }
}

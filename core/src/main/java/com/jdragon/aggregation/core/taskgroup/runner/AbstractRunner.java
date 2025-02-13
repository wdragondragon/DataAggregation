package com.jdragon.aggregation.core.taskgroup.runner;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.enums.State;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Data
public class AbstractRunner implements Runnable {
    private AbstractJobPlugin plugin;

    private Configuration jobConf;

    private long jobId;

    private Communication runnerCommunication;

    public AbstractRunner(AbstractJobPlugin plugin) {
        this.plugin = plugin;
    }

    public void setJobConf(Configuration jobConf) {
        this.jobConf = jobConf;
        this.plugin.setPluginJobConf(jobConf);
    }

    public void destroy() {
        if (this.plugin != null) {
            this.plugin.destroy();
        }
    }

    private void mark(State state) {
        this.runnerCommunication.setState(state);
        log.info("runner mark status:{}", state);
    }

    public void markRun() {
        mark(State.RUNNING);
    }

    public void markSuccess() {
        mark(State.SUCCEEDED);
    }

    public void markFail(final Throwable throwable) {
        mark(State.FAILED);
        this.runnerCommunication.setTimestamp(System.currentTimeMillis());
        this.runnerCommunication.setThrowable(throwable);
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

    }
}

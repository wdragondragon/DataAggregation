package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.jdragon.aggregation.core.statistics.communication.RunStatus;
import com.jdragon.aggregation.pluginloader.ClassLoaderSwapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobReporterExecutor {

    private final ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();

    private final AbstractJobReporter jobReporterExecInterface;

    public JobReporterExecutor(AbstractJobReporter jobReporterExecInterface) {
        this.jobReporterExecInterface = jobReporterExecInterface;
    }

    public void exec(RunStatus runStatus) {
        classLoaderSwapper.setCurrentThreadClassLoader(jobReporterExecInterface.getClassLoader());
        try {
            jobReporterExecInterface.report(runStatus);
        } catch (Throwable e) {
            log.error("上报任务状态失败：{}", e.getMessage(), e);
        } finally {
            classLoaderSwapper.restoreCurrentThreadClassLoader();
        }
    }

    public void recovery(RunStatus runStatus) {
        classLoaderSwapper.setCurrentThreadClassLoader(jobReporterExecInterface.getClassLoader());
        try {
            jobReporterExecInterface.recovery(runStatus);
        } catch (Throwable e) {
            log.error("任务状态恢复失败：{}", e.getMessage(), e);
        } finally {
            classLoaderSwapper.restoreCurrentThreadClassLoader();
        }
    }
}

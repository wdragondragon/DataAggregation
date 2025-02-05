package com.jdragon.aggregation.core.plugin;


import com.jdragon.aggregation.commons.element.Record;

public abstract class TaskPluginCollector implements PluginCollector {
    /**
     * 收集脏数据
     *
     * @param dirtyRecord  脏数据信息
     * @param t            异常信息
     * @param errorMessage 错误的提示信息
     */
    public abstract void collectDirtyRecord(final Record dirtyRecord,
                                            final Throwable t, final String errorMessage);

    /**
     * 收集脏数据
     *
     * @param dirtyRecord  脏数据信息
     * @param errorMessage 错误的提示信息
     */
    public void collectDirtyRecord(final Record dirtyRecord,
                                   final String errorMessage) {
        this.collectDirtyRecord(dirtyRecord, null, errorMessage);
    }

    /**
     * 收集脏数据
     *
     * @param dirtyRecord 脏数据信息
     * @param t           异常信息
     */
    public void collectDirtyRecord(final Record dirtyRecord, final Throwable t) {
        this.collectDirtyRecord(dirtyRecord, t, "");
    }

    /**
     * 收集自定义信息，Job插件可以通过getMessage获取该信息 <br >
     * 如果多个key冲突，内部使用List记录同一个key，多个value情况。<br >
     */
    public abstract void collectMessage(final String key, final String value);
}

package com.jdragon.aggregation.core.statistics.communication;

import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class Communication {
    /**
     * 所有的数值key-value对 *
     */
    private Map<String, Number> counter = new ConcurrentHashMap<>();

    public synchronized void increaseCounter(final String key, final long deltaValue) {
        Validate.isTrue(StringUtils.isNotBlank(key), "增加counter的key不能为空");

        long value = this.getLongCounter(key);

        this.counter.put(key, value + deltaValue);
    }


    public synchronized Long getLongCounter(final String key) {
        Number value = this.counter.get(key);

        return value == null ? 0 : value.longValue();
    }

    public synchronized void setLongCounter(final String key, final long value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "设置counter的key不能为空");
        this.counter.put(key, value);
    }

    public synchronized Double getDoubleCounter(final String key) {
        Number value = this.counter.get(key);

        return value == null ? 0.0d : value.doubleValue();
    }

    public synchronized void setDoubleCounter(final String key, final double value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "设置counter的key不能为空");
        this.counter.put(key, value);
    }

}

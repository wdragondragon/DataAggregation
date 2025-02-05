package com.jdragon.aggregation.core.statistics.communication;

import com.jdragon.aggregation.core.enums.State;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.jdragon.aggregation.core.statistics.communication.CommunicationTool.*;

@Data
public class Communication {

    /**
     * 运行状态 *
     */
    private State state;

    /**
     * 所有的数值key-value对 *
     */
    private Map<String, Number> counter = new ConcurrentHashMap<>();

    /**
     * 异常记录 *
     */
    private Throwable throwable;

    /**
     * 记录的timestamp *
     */
    private long timestamp;

    /**
     * task给job的信息 *
     */
    Map<String, List<String>> message;

    public Communication() {
        this.init();
    }

    public synchronized void reset() {
        this.init();
    }

    private void init() {
        this.counter = new ConcurrentHashMap<>();
        this.state = State.RUNNING;
        this.throwable = null;
        this.message = new ConcurrentHashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

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

    public synchronized void addMessage(final String key, final String value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "增加message的key不能为空");
        List<String> valueList = this.message.computeIfAbsent(key, k -> new ArrayList<>());
        valueList.add(value);
    }

    public long getTotalReadRecords() {
        return getLongCounter(READ_SUCCEED_RECORDS) +
                getLongCounter(READ_FAILED_RECORDS);
    }

    public long getTotalReadBytes() {
        return getLongCounter(READ_SUCCEED_BYTES) +
                getLongCounter(READ_FAILED_BYTES);
    }

    public long getTotalErrorRecords() {
        return getLongCounter(READ_FAILED_RECORDS) +
                getLongCounter(WRITE_FAILED_RECORDS);
    }

    public long getTotalErrorBytes() {
        return getLongCounter(READ_FAILED_BYTES) +
                getLongCounter(WRITE_FAILED_BYTES);
    }

    public Communication clone() {
        Communication newCommunication = new Communication();
        newCommunication.setState(State.SUCCEEDED);
        newCommunication.mergeFrom(this);
        return newCommunication;
    }

    public synchronized Communication mergeFrom(final Communication otherComm) {
        if (otherComm == null) {
            return this;
        }

        /**
         * counter的合并，将otherComm的值累加到this中，不存在的则创建
         * 同为long
         */
        for (Map.Entry<String, Number> entry : otherComm.getCounter().entrySet()) {
            String key = entry.getKey();
            Number otherValue = entry.getValue();
            if (otherValue == null) {
                continue;
            }

            Number value = this.counter.get(key);
            if (value == null) {
                value = otherValue;
            } else {
                if (value instanceof Long && otherValue instanceof Long) {
                    value = value.longValue() + otherValue.longValue();
                } else {
                    value = value.doubleValue() + value.doubleValue();
                }
            }

            this.counter.put(key, value);
        }

        // 合并state
        mergeStateFrom(otherComm);

        /**
         * 合并throwable，当this的throwable为空时，
         * 才将otherComm的throwable合并进来
         */
        this.throwable = this.throwable == null ? otherComm.getThrowable() : this.throwable;

        /**
         * timestamp是整个一次合并的时间戳，单独两两communication不作合并
         */

        /**
         * message的合并采取求并的方式，即全部累计在一起
         */
        for (Map.Entry<String, List<String>> entry : otherComm.getMessage().entrySet()) {
            String key = entry.getKey();
            List<String> valueList = this.message.get(key);
            if (valueList == null) {
                valueList = new ArrayList<String>();
                this.message.put(key, valueList);
            }

            valueList.addAll(entry.getValue());
        }

        return this;
    }

    /**
     * 合并state，优先级： (Failed | Killed) > Running > Success
     * 这里不会出现 Killing 状态，killing 状态只在 Job 自身状态上才有.
     */
    public synchronized State mergeStateFrom(final Communication otherComm) {
        State retState = this.getState();
        if (otherComm == null) {
            return retState;
        }

        if (this.state == State.FAILED || otherComm.getState() == State.FAILED
                || this.state == State.KILLED || otherComm.getState() == State.KILLED) {
            retState = State.FAILED;
        } else if (this.state.isRunning() || otherComm.state.isRunning()) {
            retState = State.RUNNING;
        }

        this.setState(retState);
        return retState;
    }

    public synchronized boolean isFinished() {
        return this.state == State.SUCCEEDED || this.state == State.FAILED
                || this.state == State.KILLED;
    }


}

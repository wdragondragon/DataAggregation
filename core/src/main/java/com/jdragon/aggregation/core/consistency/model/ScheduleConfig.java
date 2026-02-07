package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

@Data
public class ScheduleConfig {
    
    private ScheduleType scheduleType;
    
    private String cronExpression;
    
    private boolean enabled = true;
    
    public enum ScheduleType {
        MANUAL,
        CRON,
        FIXED_RATE,
        FIXED_DELAY
    }
}
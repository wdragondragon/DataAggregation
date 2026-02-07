package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConsistencyRuleManager {
    
    private final Map<String, ConsistencyRule> ruleStore = new ConcurrentHashMap<>();
    
    private final String ruleStoragePath;
    
    public ConsistencyRuleManager() {
        this.ruleStoragePath = null;
    }
    
    public ConsistencyRuleManager(String ruleStoragePath) {
        this.ruleStoragePath = ruleStoragePath;
        loadRulesFromStorage();
    }
    
    public void addRule(ConsistencyRule rule) {
        if (rule.getRuleId() == null || rule.getRuleId().trim().isEmpty()) {
            throw new IllegalArgumentException("Rule ID cannot be null or empty");
        }
        
        ruleStore.put(rule.getRuleId(), rule);
        log.info("Added consistency rule: {} - {}", rule.getRuleId(), rule.getRuleName());
        
        saveRuleToStorage(rule);
    }
    
    public void updateRule(ConsistencyRule rule) {
        if (!ruleStore.containsKey(rule.getRuleId())) {
            throw new IllegalArgumentException("Rule not found: " + rule.getRuleId());
        }
        
        ruleStore.put(rule.getRuleId(), rule);
        log.info("Updated consistency rule: {} - {}", rule.getRuleId(), rule.getRuleName());
        
        saveRuleToStorage(rule);
    }
    
    public void deleteRule(String ruleId) {
        ConsistencyRule removed = ruleStore.remove(ruleId);
        if (removed != null) {
            log.info("Deleted consistency rule: {} - {}", ruleId, removed.getRuleName());
            deleteRuleFromStorage(ruleId);
        }
    }
    
    public ConsistencyRule getRule(String ruleId) {
        return ruleStore.get(ruleId);
    }
    
    public Map<String, ConsistencyRule> getAllRules() {
        return new HashMap<>(ruleStore);
    }
    
    public boolean ruleExists(String ruleId) {
        return ruleStore.containsKey(ruleId);
    }
    
    public void enableRule(String ruleId) {
        ConsistencyRule rule = getRule(ruleId);
        if (rule != null) {
            rule.setEnabled(true);
            updateRule(rule);
        }
    }
    
    public void disableRule(String ruleId) {
        ConsistencyRule rule = getRule(ruleId);
        if (rule != null) {
            rule.setEnabled(false);
            updateRule(rule);
        }
    }
    
    private void loadRulesFromStorage() {
        if (ruleStoragePath == null) {
            return;
        }
        
        // TODO: Implement loading rules from file system or database
        log.info("Loading rules from storage: {}", ruleStoragePath);
    }
    
    private void saveRuleToStorage(ConsistencyRule rule) {
        if (ruleStoragePath == null) {
            return;
        }
        
        // TODO: Implement saving rule to file system or database
        log.debug("Saving rule to storage: {}", rule.getRuleId());
    }
    
    private void deleteRuleFromStorage(String ruleId) {
        if (ruleStoragePath == null) {
            return;
        }
        
        // TODO: Implement deleting rule from storage
        log.debug("Deleting rule from storage: {}", ruleId);
    }
    
    public ConsistencyRule createRuleFromConfig(Configuration config) {
        return ConsistencyRule.fromConfig(config);
    }
}
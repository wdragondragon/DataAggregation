package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        
        log.info("Loading rules from storage: {}", ruleStoragePath);
        File storageDir = new File(ruleStoragePath);
        if (!storageDir.exists() || !storageDir.isDirectory()) {
            log.warn("Rule storage directory does not exist: {}", ruleStoragePath);
            return;
        }
        
        File[] ruleFiles = storageDir.listFiles((dir, name) -> name.endsWith(".json"));
        if (ruleFiles == null) {
            return;
        }
        
        for (File ruleFile : ruleFiles) {
            try {
                Configuration config = Configuration.from(ruleFile);
                ConsistencyRule rule = ConsistencyRule.fromConfig(config);
                ruleStore.put(rule.getRuleId(), rule);
                log.debug("Loaded rule from file: {} -> {}", ruleFile.getName(), rule.getRuleId());
            } catch (Exception e) {
                log.error("Failed to load rule from file: {}", ruleFile.getName(), e);
            }
        }
        log.info("Loaded {} rules from storage", ruleStore.size());
    }
    
    private void saveRuleToStorage(ConsistencyRule rule) {
        if (ruleStoragePath == null) {
            return;
        }
        
        log.debug("Saving rule to storage: {}", rule.getRuleId());
        try {
            File storageDir = new File(ruleStoragePath);
            if (!storageDir.exists()) {
                storageDir.mkdirs();
            }
            
            File ruleFile = new File(storageDir, rule.getRuleId() + ".json");
            String json = toStorageConfiguration(rule).toJSON();
            Files.write(ruleFile.toPath(), json.getBytes(StandardCharsets.UTF_8));
            log.debug("Rule saved to file: {}", ruleFile.getAbsolutePath());
        } catch (Exception e) {
            log.error("Failed to save rule to storage: {}", rule.getRuleId(), e);
        }
    }
    
    private void deleteRuleFromStorage(String ruleId) {
        if (ruleStoragePath == null) {
            return;
        }
        
        log.debug("Deleting rule from storage: {}", ruleId);
        try {
            File storageDir = new File(ruleStoragePath);
            File ruleFile = new File(storageDir, ruleId + ".json");
            if (ruleFile.exists()) {
                Files.delete(ruleFile.toPath());
                log.debug("Rule file deleted: {}", ruleFile.getAbsolutePath());
            }
        } catch (Exception e) {
            log.error("Failed to delete rule from storage: {}", ruleId, e);
        }
    }
    
    public ConsistencyRule createRuleFromConfig(Configuration config) {
        return ConsistencyRule.fromConfig(config);
    }

    private Configuration toStorageConfiguration(ConsistencyRule rule) {
        Configuration config = rule.toConfig();
        List<Configuration> sources = new ArrayList<Configuration>();
        for (DataSourceConfig dataSource : rule.getDataSources()) {
            Configuration sourceConfig = Configuration.newDefault();
            sourceConfig.set("sourceId", dataSource.getSourceId());
            sourceConfig.set("sourceName", dataSource.getSourceName());
            sourceConfig.set("pluginName", dataSource.getPluginName());
            sourceConfig.set("connectionConfig", dataSource.getConnectionConfig());
            sourceConfig.set("querySql", dataSource.getQuerySql());
            sourceConfig.set("tableName", dataSource.getTableName());
            sourceConfig.set("confidenceWeight", dataSource.getConfidenceWeight());
            sourceConfig.set("priority", dataSource.getPriority());
            sourceConfig.set("maxRecords", dataSource.getMaxRecords());
            sourceConfig.set("fieldMappings", dataSource.getFieldMappings());
            sourceConfig.set("extConfig", dataSource.getExtConfig());
            sourceConfig.set("updateTarget", dataSource.getUpdateTarget());
            sources.add(sourceConfig);
        }
        config.set("dataSources", sources);
        return config;
    }
}

package com.jdragon.aggregation.core.consistency.i18n;

import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;

import java.text.MessageFormat;
import java.util.*;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * 多语言消息资源管理器
 * 支持英文、中文和双语模式
 */
public class MessageResource {
    
    private static final Map<String, ResourceBundle> bundles = new HashMap<>();
    
    /**
     * UTF-8控制类，用于正确读取UTF-8编码的属性文件
     */
    private static class UTF8Control extends ResourceBundle.Control {
        @Override
        public ResourceBundle newBundle(String baseName, Locale locale, String format, 
                ClassLoader loader, boolean reload) throws IllegalAccessException, 
                InstantiationException, IOException {
            String bundleName = toBundleName(baseName, locale);
            String resourceName = toResourceName(bundleName, "properties");
            
            try (InputStream stream = loader.getResourceAsStream(resourceName)) {
                if (stream != null) {
                    try (InputStreamReader reader = new InputStreamReader(stream, "UTF-8")) {
                        return new PropertyResourceBundle(reader);
                    }
                }
            } catch (UnsupportedEncodingException e) {
                // UTF-8应该总是可用的
                throw new RuntimeException("UTF-8 encoding not supported", e);
            }
            return super.newBundle(baseName, locale, format, loader, reload);
        }
        
        @Override
        public Locale getFallbackLocale(String baseName, Locale locale) {
            // 禁用回退，确保只加载请求的特定语言版本
            return null;
        }
    }
    
    static {
        // 初始化资源包，使用UTF-8控制类
        UTF8Control utf8Control = new UTF8Control();
        // 英文使用根区域（无后缀的messages.properties）
        bundles.put("en", ResourceBundle.getBundle("messages", Locale.ROOT, utf8Control));
        // 中文使用简体中文区域
        bundles.put("zh", ResourceBundle.getBundle("messages", Locale.SIMPLIFIED_CHINESE, utf8Control));
    }
    
    private final OutputConfig.ReportLanguage language;
    private final MessageFormat englishFormatter;
    private final MessageFormat chineseFormatter;
    
    public MessageResource(OutputConfig.ReportLanguage language) {
        this.language = language;
        this.englishFormatter = new MessageFormat("");
        this.englishFormatter.setLocale(Locale.ENGLISH);
        this.chineseFormatter = new MessageFormat("");
        this.chineseFormatter.setLocale(Locale.SIMPLIFIED_CHINESE);
    }
    
    /**
     * 获取消息文本
     */
    public String getMessage(String key, Object... args) {
        switch (language) {
            case CHINESE:
                return getChineseMessage(key, args);
            case BILINGUAL:
                return getBilingualMessage(key, args);
            case ENGLISH:
            default:
                return getEnglishMessage(key, args);
        }
    }
    
    /**
     * 获取英文消息
     */
    public String getEnglishMessage(String key, Object... args) {
        String pattern = getPattern("en", key);
        englishFormatter.applyPattern(pattern);
        return englishFormatter.format(args);
    }
    
    /**
     * 获取中文消息
     */
    public String getChineseMessage(String key, Object... args) {
        String pattern = getPattern("zh", key);
        chineseFormatter.applyPattern(pattern);
        return chineseFormatter.format(args);
    }
    
    /**
     * 获取双语消息
     */
    public String getBilingualMessage(String key, Object... args) {
        String english = getEnglishMessage(key, args);
        String chinese = getChineseMessage(key, args);
        
        // 对于双语模式，格式为: 英文 / 中文
        if (english.equals(chinese) || chinese.isEmpty()) {
            return english;
        }
        return english + " / " + chinese;
    }
    
    /**
     * 获取字段名翻译
     */
    public String getFieldName(String fieldName) {
        String key = "field." + fieldName.toLowerCase();
        switch (language) {
            case CHINESE:
                return getPattern("zh", key, fieldName);
            case BILINGUAL:
                String english = getPattern("en", key, toDisplayFieldName(fieldName));
                String chinese = getPattern("zh", key, fieldName);
                if (chinese.equals(english)) {
                    return english;
                }
                return english + " / " + chinese;
            case ENGLISH:
            default:
                return getPattern("en", key, toDisplayFieldName(fieldName));
        }
    }
    
    /**
     * 将字段名转换为可读的显示名称（下划线转空格，单词首字母大写）
     */
    private String toDisplayFieldName(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return fieldName;
        }
        // 将下划线替换为空格
        String withSpaces = fieldName.replace('_', ' ');
        // 单词首字母大写
        StringBuilder result = new StringBuilder();
        boolean capitalizeNext = true;
        for (char c : withSpaces.toCharArray()) {
            if (Character.isWhitespace(c)) {
                result.append(c);
                capitalizeNext = true;
            } else if (capitalizeNext) {
                result.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    /**
     * 获取冲突类型翻译
     */
    public String getConflictType(String conflictType) {
        String normalized = conflictType;
        if (normalized.endsWith("_CONFLICT")) {
            normalized = normalized.substring(0, normalized.length() - "_CONFLICT".length());
        }
        String key = "conflict.type." + normalized.toLowerCase().replace('_', '.');
        switch (language) {
            case CHINESE:
                return getPattern("zh", key, conflictType);
            case BILINGUAL:
                String english = getPattern("en", key, conflictType);
                String chinese = getPattern("zh", key, conflictType);
                if (chinese.equals(english)) {
                    return english;
                }
                return english + " / " + chinese;
            case ENGLISH:
            default:
                return getPattern("en", key, conflictType);
        }
    }

    /**
     * 获取解决策略翻译
     */
    public String getStrategy(ConflictResolutionStrategy strategy) {
        String key = "strategy." + strategy.name().toLowerCase().replace('_', '.');
        switch (language) {
            case CHINESE:
                return getPattern("zh", key, strategy.getDescription());
            case BILINGUAL:
                String english = getPattern("en", key, strategy.name());
                String chinese = getPattern("zh", key, strategy.getDescription());
                if (chinese.equals(english)) {
                    return english;
                }
                return english + " / " + chinese;
            case ENGLISH:
            default:
                return getPattern("en", key, strategy.name());
        }
    }

    

    
    /**
     * 获取模式字符串，带默认值
     */
    private String getPattern(String lang, String key, String defaultValue) {
        try {
            ResourceBundle bundle = bundles.get(lang);
            if (bundle != null && bundle.containsKey(key)) {
                return bundle.getString(key);
            }
        } catch (MissingResourceException e) {
            // 资源未找到，返回默认值
        }
        return defaultValue;
    }
    
    private String getPattern(String lang, String key) {
        return getPattern(lang, key, "[" + key + "]");
    }
    
    /**
     * 检查是否包含指定键
     */
    public boolean containsKey(String key) {
        return bundles.get("en").containsKey(key) || bundles.get("zh").containsKey(key);
    }
    
    /**
     * 获取当前语言
     */
    public OutputConfig.ReportLanguage getLanguage() {
        return language;
    }
    
    /**
     * 获取双语消息（仅用于显示）
     */
    public String getDisplayMessage(String key, Object... args) {
        switch (language) {
            case CHINESE:
                return getChineseMessage(key, args);
            case BILINGUAL:
                return getBilingualMessage(key, args);
            case ENGLISH:
            default:
                return getEnglishMessage(key, args);
        }
    }
    
    /**
     * 静态工厂方法
     */
    public static MessageResource forLanguage(OutputConfig.ReportLanguage language) {
        return new MessageResource(language);
    }
}
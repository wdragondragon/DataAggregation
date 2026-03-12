package com.jdragon.aggregation.core.consistency.service.plugin;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.service.BaseConflictResolver;
import com.jdragon.aggregation.core.consistency.service.ConflictResolver;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ConflictResolverRegistryTest {

    private ConflictResolverRegistry registry;

    @Before
    public void setUp() {
        // Get fresh instance for each test
        registry = ConflictResolverRegistry.getInstance();
        // Clear registry to start with clean state for each test
        registry.clear();
    }

    @Test
    public void testSingletonInstance() {
        ConflictResolverRegistry instance1 = ConflictResolverRegistry.getInstance();
        ConflictResolverRegistry instance2 = ConflictResolverRegistry.getInstance();
        assertSame("Should return same instance for singleton", instance1, instance2);
    }

    @Test
    public void testBuiltInResolversInitialized() {
        // After clear(), built-in resolvers are re-initialized
        Set<String> strategies = registry.getRegisteredStrategies();
        assertTrue("Should contain HIGH_CONFIDENCE", strategies.contains("HIGH_CONFIDENCE"));
        assertTrue("Should contain WEIGHTED_AVERAGE", strategies.contains("WEIGHTED_AVERAGE"));
        assertTrue("Should contain MAJORITY_VOTE", strategies.contains("MAJORITY_VOTE"));
        assertTrue("Should contain MANUAL_REVIEW", strategies.contains("MANUAL_REVIEW"));
    }

    @Test
    public void testRegisterResolverClass() {
        registry.registerResolverClass("MY_CUSTOM_STRATEGY", TestConflictResolver.class.getName());

        assertTrue("Strategy should be registered", registry.hasStrategy("MY_CUSTOM_STRATEGY"));
        assertTrue("Strategy should be case-insensitive", registry.hasStrategy("my_custom_strategy"));

        // Create resolver instance
        List<DataSourceConfig> dataSourceConfigs = Collections.emptyList();
        Map<String, Object> params = Collections.emptyMap();
        ConflictResolver resolver = registry.createResolver("MY_CUSTOM_STRATEGY", dataSourceConfigs, params);

        assertNotNull("Resolver should be created", resolver);
        assertTrue("Resolver should be instance of TestConflictResolver", 
                   resolver instanceof TestConflictResolver);
    }

    @Test
    public void testRegisterResolverInstance() {
        ConflictResolver testResolver = new TestConflictResolver(Collections.emptyList());
        registry.registerResolverInstance("INSTANCE_STRATEGY", testResolver);

        assertTrue("Strategy should be registered", registry.hasStrategy("INSTANCE_STRATEGY"));

        List<DataSourceConfig> dataSourceConfigs = Collections.emptyList();
        Map<String, Object> params = Collections.emptyMap();
        ConflictResolver resolver = registry.createResolver("INSTANCE_STRATEGY", dataSourceConfigs, params);

        assertSame("Should return same instance", testResolver, resolver);
    }

    @Test
    public void testCreateResolverForBuiltInStrategy() {
        List<DataSourceConfig> dataSourceConfigs = Collections.emptyList();
        Map<String, Object> params = Collections.emptyMap();

        ConflictResolver resolver = registry.createResolver("HIGH_CONFIDENCE", dataSourceConfigs, params);
        assertNotNull("Should create resolver for built-in strategy", resolver);
        
        resolver = registry.createResolver("WEIGHTED_AVERAGE", dataSourceConfigs, params);
        assertNotNull("Should create resolver for WEIGHTED_AVERAGE", resolver);
        
        resolver = registry.createResolver("MAJORITY_VOTE", dataSourceConfigs, params);
        assertNotNull("Should create resolver for MAJORITY_VOTE", resolver);
        
        resolver = registry.createResolver("MANUAL_REVIEW", dataSourceConfigs, params);
        assertNotNull("Should create resolver for MANUAL_REVIEW", resolver);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateResolverForNonExistentStrategy() {
        List<DataSourceConfig> dataSourceConfigs = Collections.emptyList();
        Map<String, Object> params = Collections.emptyMap();
        registry.createResolver("NON_EXISTENT_STRATEGY", dataSourceConfigs, params);
    }

    @Test
    public void testBackwardCompatibilityCustomRule() {
        Map<String, Object> params = new HashMap<>();
        params.put("customClass", TestConflictResolver.class.getName());

        List<DataSourceConfig> dataSourceConfigs = Collections.emptyList();
        ConflictResolver resolver = registry.createResolver("CUSTOM_RULE", dataSourceConfigs, params);

        assertNotNull("Should create resolver via CUSTOM_RULE backward compatibility", resolver);
        assertTrue("Resolver should be instance of TestConflictResolver", 
                   resolver instanceof TestConflictResolver);
    }

    @Test
    public void testGetRegisteredStrategies() {
        Set<String> strategies = registry.getRegisteredStrategies();
        assertNotNull("Strategies set should not be null", strategies);
        assertTrue("Should contain built-in strategies", strategies.size() >= 4);
        
        registry.registerResolverClass("EXTRA_STRATEGY", TestConflictResolver.class.getName());
        strategies = registry.getRegisteredStrategies();
        assertTrue("Should contain newly registered strategy", strategies.contains("EXTRA_STRATEGY"));
    }

    @Test
    public void testUnregisterStrategy() {
        registry.registerResolverClass("TEMPORARY_STRATEGY", TestConflictResolver.class.getName());
        assertTrue("Strategy should be registered", registry.hasStrategy("TEMPORARY_STRATEGY"));
        
        registry.unregisterStrategy("TEMPORARY_STRATEGY");
        assertFalse("Strategy should be unregistered", registry.hasStrategy("TEMPORARY_STRATEGY"));
    }

    @Test
    public void testClearRegistry() {
        registry.registerResolverClass("EXTRA1", TestConflictResolver.class.getName());
        registry.registerResolverClass("EXTRA2", TestConflictResolver.class.getName());
        
        registry.clear();
        
        // After clear, only built-in strategies should remain
        Set<String> strategies = registry.getRegisteredStrategies();
        assertTrue("Should contain built-in strategies", strategies.contains("HIGH_CONFIDENCE"));
        assertFalse("Should not contain EXTRA1", strategies.contains("EXTRA1"));
        assertFalse("Should not contain EXTRA2", strategies.contains("EXTRA2"));
    }

    @Test
    public void testRegisterResolverProvider() {
        TestConflictResolverProvider provider = new TestConflictResolverProvider();
        registry.registerResolverProvider(provider);

        assertTrue("Strategy should be registered", registry.hasStrategy("TEST_PROVIDER_STRATEGY"));

        List<DataSourceConfig> dataSourceConfigs = Collections.emptyList();
        Map<String, Object> params = Collections.emptyMap();
        ConflictResolver resolver = registry.createResolver("TEST_PROVIDER_STRATEGY", dataSourceConfigs, params);

        assertNotNull("Resolver should be created via provider", resolver);
        assertEquals("Resolver should have correct strategy", 
                     ConflictResolutionStrategy.CUSTOM_RULE, 
                     resolver.getStrategy());
    }

    @Test
    public void testStrategyNameCaseInsensitivity() {
        registry.registerResolverClass("MY_STRATEGY", TestConflictResolver.class.getName());
        
        assertTrue("Should recognize uppercase", registry.hasStrategy("MY_STRATEGY"));
        assertTrue("Should recognize lowercase", registry.hasStrategy("my_strategy"));
        assertTrue("Should recognize mixed case", registry.hasStrategy("My_Strategy"));
        
        // Should be able to create resolver with any case
        List<DataSourceConfig> dataSourceConfigs = Collections.emptyList();
        Map<String, Object> params = Collections.emptyMap();
        ConflictResolver resolver = registry.createResolver("my_strategy", dataSourceConfigs, params);
        assertNotNull("Should create resolver with lowercase name", resolver);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterResolverClassWithNullStrategyName() {
        registry.registerResolverClass(null, TestConflictResolver.class.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterResolverClassWithNullClassName() {
        registry.registerResolverClass("MY_STRATEGY", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterResolverInstanceWithNullStrategyName() {
        registry.registerResolverInstance(null, new TestConflictResolver(Collections.emptyList()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterResolverInstanceWithNullResolver() {
        registry.registerResolverInstance("MY_STRATEGY", null);
    }

    @Test
    public void testOverrideExistingStrategy() {
        registry.registerResolverClass("MY_STRATEGY", TestConflictResolver.class.getName());
        assertTrue("Strategy should be registered", registry.hasStrategy("MY_STRATEGY"));
        
        // Override with different class (should log warning but succeed)
        registry.registerResolverClass("MY_STRATEGY", AnotherTestResolver.class.getName());
        assertTrue("Strategy should still be registered", registry.hasStrategy("MY_STRATEGY"));
    }

    // ========== Test Implementations ==========

    public static class TestConflictResolver extends BaseConflictResolver {
        public TestConflictResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }

        @Override
        public ResolutionResult resolve(DifferenceRecord differenceRecord) {
            ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.CUSTOM_RULE);
            result.setManuallyReviewed(false);
            return result;
        }

        @Override
        public ConflictResolutionStrategy getStrategy() {
            return ConflictResolutionStrategy.CUSTOM_RULE;
        }
    }

    public static class AnotherTestResolver extends BaseConflictResolver {
        public AnotherTestResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }

        @Override
        public ResolutionResult resolve(DifferenceRecord differenceRecord) {
            ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.CUSTOM_RULE);
            result.setManuallyReviewed(false);
            return result;
        }

        @Override
        public ConflictResolutionStrategy getStrategy() {
            return ConflictResolutionStrategy.CUSTOM_RULE;
        }
    }

    public static class TestConflictResolverProvider implements ConflictResolverProvider {
        @Override
        public String getStrategyName() {
            return "TEST_PROVIDER_STRATEGY";
        }

        @Override
        public String getDescription() {
            return "Test provider for unit testing";
        }

        @Override
        public ConflictResolver createResolver(List<DataSourceConfig> dataSourceConfigs,
                                              Map<String, Object> resolutionParams) {
            return new TestConflictResolver(dataSourceConfigs);
        }
    }
}
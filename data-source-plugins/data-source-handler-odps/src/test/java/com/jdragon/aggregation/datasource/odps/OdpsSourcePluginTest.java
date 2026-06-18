package com.jdragon.aggregation.datasource.odps;

import com.aliyun.odps.Odps;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OdpsSourcePluginTest {

    @Test
    public void connectTestShouldProbeResolvedProject() {
        TestableOdpsSourcePlugin plugin = new TestableOdpsSourcePlugin();
        BaseDataSourceDTO dataSource = dataSource();

        assertTrue(plugin.connectTest(dataSource));

        assertEquals("project_dev", plugin.defaultProject);
        assertEquals(1, plugin.probeCount);
    }

    @Test(expected = IllegalArgumentException.class)
    public void connectTestShouldRejectMissingEndpoint() {
        BaseDataSourceDTO dataSource = dataSource();
        dataSource.setHost("");

        new TestableOdpsSourcePlugin().connectTest(dataSource);
    }

    @Test(expected = IllegalArgumentException.class)
    public void connectTestShouldRejectMissingProject() {
        BaseDataSourceDTO dataSource = dataSource();
        dataSource.setDatabase("");

        new TestableOdpsSourcePlugin().connectTest(dataSource);
    }

    private static BaseDataSourceDTO dataSource() {
        BaseDataSourceDTO dataSource = new BaseDataSourceDTO();
        dataSource.setType("odps");
        dataSource.setHost("http://service.cn-hangzhou.maxcompute.aliyun.com/api");
        dataSource.setDatabase("project");
        dataSource.setUserName("access-key-id");
        dataSource.setPassword("access-key-secret");
        Map<String, String> extraParams = new LinkedHashMap<>();
        extraParams.put("projectEnv", "dev");
        dataSource.setExtraParams(extraParams);
        return dataSource;
    }

    private static class TestableOdpsSourcePlugin extends OdpsSourcePlugin {

        private String defaultProject;

        private int probeCount;

        @Override
        protected Odps createOdpsClient(BaseDataSourceDTO dataSource) {
            return null;
        }

        @Override
        protected boolean probeConnection(Odps odps) {
            probeCount++;
            return true;
        }

        @Override
        protected void setDefaultProject(Odps odps, String project) {
            defaultProject = project;
        }
    }
}

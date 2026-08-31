package com.jdragon.aggregation.writer.kafka;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.queue.kafka.KafkaAuthUtil;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaWriterPropertiesTest {

    @Test
    public void dedicatedWriterAndDatasourceOptionsOverrideOtherProperties() throws Exception {
        KafkaWriter writer = new KafkaWriter();
        Configuration datasourceConfig = Configuration.newDefault();
        datasourceConfig.set("security.protocol", "SSL");

        Map<String, String> otherProperties = new LinkedHashMap<String, String>();
        otherProperties.put("bootstrap.servers", "wrong-broker:9092");
        otherProperties.put("acks", "0");
        otherProperties.put("retries", "99");
        otherProperties.put("batch.size", "1");
        otherProperties.put("security.protocol", "PLAINTEXT");
        otherProperties.put("linger.ms", "25");

        setField(writer, "conf", datasourceConfig);
        setField(writer, "bootstrapServers", "broker:9092");
        setField(writer, "acks", "all");
        setField(writer, "retries", "3");
        setField(writer, "batchSize", "2048");
        setField(writer, "otherProperties", otherProperties);

        Method getProperties = KafkaWriter.class.getDeclaredMethod("getProperties");
        getProperties.setAccessible(true);
        Properties properties = (Properties) getProperties.invoke(writer);

        Assert.assertEquals("broker:9092", properties.getProperty("bootstrap.servers"));
        Assert.assertEquals("all", properties.getProperty("acks"));
        Assert.assertEquals("3", properties.getProperty("retries"));
        Assert.assertEquals("2048", properties.getProperty("batch.size"));
        Assert.assertEquals("SSL", properties.getProperty("security.protocol"));
        Assert.assertEquals("25", properties.getProperty("linger.ms"));
    }

    @Test
    public void writerBootstrapAndDatasourceSecuritySettingsHaveStablePrecedence() throws Exception {
        KafkaWriter writer = new KafkaWriter();
        Configuration datasourceConfig = Configuration.newDefault();
        datasourceConfig.set("bootstrap.servers", "datasource-broker:9092");
        datasourceConfig.set("username", "user");
        datasourceConfig.set("password", "password");
        datasourceConfig.set("security.protocol", "SASL_SSL");
        datasourceConfig.set("sasl.mechanism", "SCRAM-SHA-512");

        setField(writer, "conf", datasourceConfig);
        setField(writer, "bootstrapServers", "task-broker:9092");
        setField(writer, "acks", "all");
        setField(writer, "retries", "0");
        setField(writer, "batchSize", "16384");
        setField(writer, "otherProperties", new LinkedHashMap<String, String>());

        Method getProperties = KafkaWriter.class.getDeclaredMethod("getProperties");
        getProperties.setAccessible(true);
        Properties properties = (Properties) getProperties.invoke(writer);

        Assert.assertEquals("task-broker:9092", properties.getProperty("bootstrap.servers"));
        Assert.assertEquals("SASL_SSL", properties.getProperty("security.protocol"));
        Assert.assertEquals("SCRAM-SHA-512", properties.getProperty("sasl.mechanism"));
        Assert.assertTrue(properties.getProperty("sasl.jaas.config").contains("ScramLoginModule"));
        Assert.assertTrue(properties.getProperty("sasl.jaas.config").contains("username=\"user\""));
    }

    @Test
    public void kerberosLoginReportsMissingRequiredCredentialsClearly() throws Exception {
        try {
            KafkaAuthUtil.login(new Properties(), "", "", "", "");
            Assert.fail("Expected missing Kerberos credentials to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("principal"));
        }
    }

    private static void setField(KafkaWriter writer, String name, Object value) throws Exception {
        Field field = KafkaWriter.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(writer, value);
    }
}

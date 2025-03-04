package test;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class CustomTest {
    private static final Logger log = LoggerFactory.getLogger(CustomTest.class);

    public static void main(String[] args) {
        Configuration configuration = Configuration.newDefault();
        long jobId = 1;

        Configuration custom = Configuration.from(new HashMap<String, Object>() {{
            put("type", "custom");
            put("config", new HashMap<>());
        }});

        configuration.set("jobId", jobId);
        configuration.set("reader", custom);
        configuration.set("writer", custom);

        JobContainer container = new JobContainer(configuration);
        container.addConsumerPlugin(PluginType.READER, new Reader.Job() {
            @Override
            public void init() {
                log.info("reader init");
            }

            @Override
            public void startRead(RecordSender recordSender) {
                log.info("reader startRead");
                Record record = new DefaultRecord();
                record.setColumn(0, new StringColumn("123"));
                recordSender.sendToWriter(record);
            }

            @Override
            public void post() {
                log.info("reader post");
            }
        });

        container.addConsumerPlugin(PluginType.WRITER, new Writer.Job() {
            @Override
            public void init() {
                log.info("writer init");
            }

            @Override
            public void startWrite(RecordReceiver recordReceiver) {
                log.info("writer startWrite");
                Record record = null;
                while ((record = recordReceiver.getFromReader()) != null) {
                    Column column = record.getColumn(0);
                    log.info("get from reader:{}", column.asString());
                }
            }

            @Override
            public void post() {
                log.info("writer post");
            }
        });
        container.start();
    }
}

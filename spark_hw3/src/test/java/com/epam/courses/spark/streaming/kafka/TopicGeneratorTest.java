package com.epam.courses.spark.streaming.kafka;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import com.epam.courses.spark.streaming.serde.KafkaJsonMonitoringRecordSerDe;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class TopicGeneratorTest {
    MockProducer<String, MonitoringRecord> producer;

    @Before
    public void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new KafkaJsonMonitoringRecordSerDe());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testEmptyRecord() {
        TopicGenerator.pushRecord(producer, "test", "");

        List<ProducerRecord<String, MonitoringRecord>> history = producer.history();
        List<ProducerRecord<String, MonitoringRecord>> expected = new ArrayList<>();
        Assert.assertEquals("Didn't match expected", expected, history);
    }

    @Test()
    public void testDeviceRecord() {
        String deviceRecordStr = "10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12";
        TopicGenerator.pushRecord(producer, "test", deviceRecordStr);

        List<ProducerRecord<String, MonitoringRecord>> history = producer.history();

        String[] lines = deviceRecordStr.split(",");
        MonitoringRecord record = new MonitoringRecord(lines);
        String deviceId = KafkaHelper.getKey(record);
        List<ProducerRecord<String, MonitoringRecord>> expected = new ArrayList();
        expected.add(new ProducerRecord<>("test", deviceId, record));

        for (int i = 0; i < expected.size(); i++) {
            MonitoringRecord expectedRecord = expected.get(i).value();
            MonitoringRecord actualRecord = history.get(i).value();
            Assert.assertEquals("Didn't match expected", expectedRecord.toString(), actualRecord.toString());
        }
    }


    @Test()
    public void testDeviceRecords() {
        List<String> devices = new ArrayList<>();
        devices.add("10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12");
        devices.add("10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2016-01-23,04:00,2016-01-23,09:00,0.035,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2016-02-18");
        devices.add("10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2016-01-23,04:00,2016-01-23,09:00,0.035,Parts per million,0.005,,,FEM,050,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2016-02-18");

        List<ProducerRecord<String, MonitoringRecord>> expected = new ArrayList();
        for (String deviceRecordStr : devices) {
            TopicGenerator.pushRecord(producer, "test", deviceRecordStr);
            String[] lines = deviceRecordStr.split(",");
            MonitoringRecord record = new MonitoringRecord(lines);
            String deviceId = KafkaHelper.getKey(record);
            expected.add(new ProducerRecord<>("test", deviceId, record));

        }
        List<ProducerRecord<String, MonitoringRecord>> history = producer.history();
        Assert.assertEquals("Didn't match expected", expected.size(), history.size());
        for (int i = 0; i < expected.size(); i++) {
            MonitoringRecord expectedRecord = expected.get(i).value();
            MonitoringRecord actualRecord = history.get(i).value();
            Assert.assertEquals("Didn't match expected", expectedRecord.toString(), actualRecord.toString());
        }

    }
}

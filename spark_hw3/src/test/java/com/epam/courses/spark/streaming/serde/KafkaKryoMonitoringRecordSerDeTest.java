package com.epam.courses.spark.streaming.serde;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaKryoMonitoringRecordSerDeTest {

    KafkaKryoMonitoringRecordSerDe serDer = null;

    @Before
    public void setUp() {
        serDer = new KafkaKryoMonitoringRecordSerDe();
        serDer.configure(null, true);
    }

    @Test()
    public void testSerializeDeserialize() {
        String topic = "myTopic";
        MonitoringRecord data = new MonitoringRecord();
        data.setCountyCode("US");
        byte[] retVal = serDer.serialize(topic, data);

        Assert.assertNotNull(retVal);
        MonitoringRecord actualData = serDer.deserialize(topic, retVal);
        Assert.assertEquals(data.getCountyCode(), actualData.getCountyCode());
    }

    @After
    public void close() {
        System.out.println("Test");
        serDer.close();
    }
}
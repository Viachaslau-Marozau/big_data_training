package com.epam.courses.spark.streaming.serde;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaJsonMonitoringRecordSerDeTest {

    KafkaJsonMonitoringRecordSerDe serDer = null;

    @Before
    public void setUp() {

        serDer = new KafkaJsonMonitoringRecordSerDe();
        serDer.configure(null, false);
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
}
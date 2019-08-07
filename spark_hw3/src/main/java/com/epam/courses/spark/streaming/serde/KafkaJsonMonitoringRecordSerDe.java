package com.epam.courses.spark.streaming.serde;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);

    private ObjectMapper objectMapper  = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        try {
            return objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        MonitoringRecord record = null;
        try {
            record = objectMapper.readValue(data, MonitoringRecord.class);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return record;
    }

    @Override
    public void close() {
        // no need for current implementation
    }
}
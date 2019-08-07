package com.epam.courses.spark.streaming.kafka;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MonitoringRecordPartitioner extends DefaultPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (value instanceof MonitoringRecord) {
            int partition = ((int) (Integer.MAX_VALUE * Math.random()) * key.hashCode()) % cluster.partitionCountForTopic(topic);
            LOGGER.debug(String.format("Partition for key: %s value: %s", key, partition));
            return partition;
        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }

    public void close() {
        // no need for current implementation
    }

    public void configure(Map<String, ?> map) {
        // no need for current implementation
    }
}
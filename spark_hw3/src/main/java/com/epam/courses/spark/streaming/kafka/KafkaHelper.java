package com.epam.courses.spark.streaming.kafka;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;

import java.util.Arrays;
import java.util.Collection;

import static com.epam.courses.spark.streaming.utils.PropertiesLoader.getKafkaConsumerProperties;
import static com.epam.courses.spark.streaming.utils.PropertiesLoader.getKafkaProducerProperties;
import static com.epam.courses.spark.streaming.utils.PropertiesLoader.getKryoProducerProperties;

public class KafkaHelper {

    public static Producer<String, MonitoringRecord> createProducer(boolean useKryoProducer)
    {
        if (useKryoProducer)
        {
            return new KafkaProducer<>(getKryoProducerProperties());
        }
        else
        {
            return new KafkaProducer<>(getKafkaProducerProperties());
        }
    }

    public static ConsumerStrategy<String, MonitoringRecord> createConsumerStrategy(String topics) {
        Collection<String> topicsList = Arrays.asList(topics.split(","));
        return ConsumerStrategies.Subscribe(topicsList, getKafkaConsumerProperties());
    }

    public static String getKey(MonitoringRecord record) {
        return record.getStateCode() + "-" + record.getCountyCode() + "-" + record.getSiteNum() + "-"
                + record.getParameterCode() + "-" + record.getPoc();
    }
}

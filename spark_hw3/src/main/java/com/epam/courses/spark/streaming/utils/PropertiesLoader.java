package com.epam.courses.spark.streaming.utils;

import com.epam.courses.spark.streaming.kafka.MonitoringRecordPartitioner;
import com.epam.courses.spark.streaming.serde.KafkaJsonMonitoringRecordSerDe;
import com.epam.courses.spark.streaming.serde.KafkaKryoMonitoringRecordSerDe;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.epam.courses.spark.streaming.utils.GlobalConstants.PROPERTIES_FILE;
import static com.epam.courses.spark.streaming.utils.GlobalConstants.BATCH_SIZE_CONFIG;
import static com.epam.courses.spark.streaming.utils.GlobalConstants.USE_KRIO_PRODUCER;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class PropertiesLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesLoader.class);

    private static Properties globalProperties = new Properties();
    private static Properties kafkaProducerProperties = new Properties();
    private static Properties kryoProducerProperties = new Properties();

    private static Map<String, Object> kafkaConsumerProperties = new HashMap<>();

    static {
        try (InputStream input = PropertiesLoader.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            globalProperties.load(input);
            LOGGER.info(String.valueOf(globalProperties));

            if (!globalProperties.isEmpty()) {
                kafkaProducerProperties.put(BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
                kafkaProducerProperties.put(ACKS_CONFIG, globalProperties.getProperty(ACKS_CONFIG));
                kafkaProducerProperties.put(RETRIES_CONFIG, globalProperties.getProperty(RETRIES_CONFIG));
                kafkaProducerProperties.put(BATCH_SIZE_CONFIG, globalProperties.getProperty(BATCH_SIZE_CONFIG));
                kafkaProducerProperties.put(LINGER_MS_CONFIG, globalProperties.getProperty(LINGER_MS_CONFIG));
                kafkaProducerProperties.put(BUFFER_MEMORY_CONFIG, globalProperties.getProperty(BUFFER_MEMORY_CONFIG));
                kafkaProducerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                kafkaProducerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class);
                kafkaProducerProperties.put(PARTITIONER_CLASS_CONFIG, MonitoringRecordPartitioner.class);

                kryoProducerProperties.put(BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
                kryoProducerProperties.put(ACKS_CONFIG, globalProperties.getProperty(ACKS_CONFIG));
                kryoProducerProperties.put(RETRIES_CONFIG, globalProperties.getProperty(RETRIES_CONFIG));
                kryoProducerProperties.put(BATCH_SIZE_CONFIG, globalProperties.getProperty(BATCH_SIZE_CONFIG));
                kryoProducerProperties.put(LINGER_MS_CONFIG, globalProperties.getProperty(LINGER_MS_CONFIG));
                kryoProducerProperties.put(BUFFER_MEMORY_CONFIG, globalProperties.getProperty(BUFFER_MEMORY_CONFIG));
                kryoProducerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                kryoProducerProperties.put(PARTITIONER_CLASS_CONFIG, MonitoringRecordPartitioner.class);
                kryoProducerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaKryoMonitoringRecordSerDe.class);

                kafkaConsumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
                kafkaConsumerProperties.put(GROUP_ID_CONFIG, globalProperties.getProperty(GROUP_ID_CONFIG));
                kafkaConsumerProperties.put(AUTO_OFFSET_RESET_CONFIG, globalProperties.getProperty(AUTO_OFFSET_RESET_CONFIG));
                kafkaConsumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, globalProperties.getProperty(ENABLE_AUTO_COMMIT_CONFIG));
                kafkaConsumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

                final boolean useKryoProducer = Boolean
                    .parseBoolean(globalProperties.getProperty(USE_KRIO_PRODUCER));
                if(useKryoProducer){
                    kafkaConsumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaKryoMonitoringRecordSerDe.class);
                }else{
                    kafkaConsumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Sorry, unable to read properties file", e);
        }
    }

    public static Properties getGlobalProperties() {
        return globalProperties;
    }

    public static Properties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

    public static Map<String, Object> getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }

    public static Properties getKryoProducerProperties() {
        return kryoProducerProperties;
    }
}

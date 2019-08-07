package com.epam.courses.spark.streaming.kafka;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import com.epam.courses.spark.streaming.utils.GlobalConstants;
import com.epam.courses.spark.streaming.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.epam.courses.spark.streaming.kafka.KafkaHelper.createProducer;

public class TopicGenerator implements GlobalConstants {

    private static final String COMMA_SEPARATOR = ",";
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                    .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final boolean useKryoProducer = Boolean
                .parseBoolean(applicationProperties.getProperty(USE_KRIO_PRODUCER));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            //Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.
            readFileAndPushRecord(sampleFile, topicName, useKryoProducer);
        }
    }

    private static void readFileAndPushRecord(String sampleFile, String topicName, boolean useKryoProducer) {
        String line;
        // no need close resources at finally block, will be closed automatically
        try (FileReader fileReader = new FileReader(sampleFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             Producer<String, MonitoringRecord> producer = createProducer(useKryoProducer);
        ) {
            while ((line = bufferedReader.readLine()) != null) {
                pushRecord(producer, topicName, line);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    protected static void pushRecord(Producer<String, MonitoringRecord> producer, String topicName, String data) {

        String[] dataArr = data.split(COMMA_SEPARATOR);

        MonitoringRecord record = new MonitoringRecord(dataArr);
        ProducerRecord<String, MonitoringRecord> producerRecord = new ProducerRecord<>(topicName, KafkaHelper.getKey(record), record);
        LOGGER.info(String.format("Prepare record: key - %s, record - %s", KafkaHelper.getKey(record), record));
        producer.send(producerRecord);
        LOGGER.info("Sent record:  key - %s, record - %s", KafkaHelper.getKey(record), record);
    }
}

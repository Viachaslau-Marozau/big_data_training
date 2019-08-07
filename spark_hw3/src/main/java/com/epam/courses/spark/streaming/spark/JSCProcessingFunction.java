package com.epam.courses.spark.streaming.spark;

import com.epam.courses.spark.streaming.htm.HTMNetwork;
import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import com.epam.courses.spark.streaming.kafka.KafkaHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.epam.courses.spark.streaming.utils.GlobalConstants.*;

public class JSCProcessingFunction implements Function0<JavaStreamingContext> {

    private static final Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> MAPPING_FUNCTION =
            new JSCMappingFunc();
    private static final Logger LOGGER = LoggerFactory.getLogger(JSCProcessingFunction.class);

    private Properties applicationProperties;
    private SparkConf sparkConf;

    public JSCProcessingFunction(Properties applicationProperties, SparkConf sparkConf) {
        this.applicationProperties = applicationProperties;
        this.sparkConf = sparkConf;
    }

    @Override
    public JavaStreamingContext call() throws Exception {

        final String rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
        final String enrichedTopic = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
        final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
        final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
        final Duration windowInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_WINDOW_DURATION_CONFIG)));
        final boolean useKryoProducer = Boolean.parseBoolean(applicationProperties.getProperty(USE_KRIO_PRODUCER));

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, batchDuration);

        jssc.checkpoint(checkpointDir);

        JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> stream =
                KafkaUtils.createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        KafkaHelper.createConsumerStrategy(rawTopicName)
                );

        JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord> stateDStream = stream
                .mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .window(windowInterval)
                .mapWithState(StateSpec.function(MAPPING_FUNCTION));

        stateDStream
                .foreachRDD(rdd -> {
                    rdd.foreachPartition(partition -> {
                        Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer(useKryoProducer);
                        partition.forEachRemaining(record -> {
                            sendRecord(producer, enrichedTopic, record);
                        });
                        producer.close();
                    });
                });
        return jssc;
    }

    private void sendRecord(Producer<String, MonitoringRecord> producer, String topicName, MonitoringRecord record) {

        ProducerRecord<String, MonitoringRecord> producerRecord = new ProducerRecord<>(topicName, KafkaHelper.getKey(record), record);
        LOGGER.debug(String.format("Prepare record: key - %s, record - %s", KafkaHelper.getKey(record), record));
        Future<RecordMetadata> future = producer.send(producerRecord);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = future.get();
            LOGGER.debug("Sent record:  key - %s, record - %s, partition=%d, offset=%d", KafkaHelper.getKey(record), record, recordMetadata.partition(), recordMetadata.offset());

        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
    }
}

package com.epam.courses.spark.streaming.spark;

import com.epam.courses.spark.streaming.utils.GlobalConstants;
import com.epam.courses.spark.streaming.utils.PropertiesLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Properties;

public class AnomalyDetector implements GlobalConstants {
    /**
     * 1. Define Spark configuration (register serializers, if needed)
     * 2. Initialize streaming context with checkpoint directory
     * 3. Read records from kafka topic "monitoring20" (com.epam.bdcc.kafka.KafkaHelper can help)
     * 4. Organized records by key and map with HTMNetwork state for each device
     * (com.epam.bdcc.kafka.KafkaHelper.getKey - unique key for the device),
     * for detecting anomalies and updating HTMNetwork state (com.epam.bdcc.spark.AnomalyDetector.mappingFunc can help)
     * 5. Send enriched records to topic "monitoringEnriched2" for further visualization
     **/
    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {

            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);

            SparkConf sparkConf = new SparkConf();
            addProperties(sparkConf, applicationProperties);

            JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, new JSCProcessingFunction(applicationProperties, sparkConf));
            try {
                streamingContext.start();
                streamingContext.awaitTermination();
            } finally {
                streamingContext.stop();
            }
        }
    }

    private static void addProperties(SparkConf sparkConf, Properties applicationProperties) {
        final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
        sparkConf.setAppName(appName);
        sparkConf.set(SPARK_MAX_RATE_PARTITION, applicationProperties.getProperty(SPARK_MAX_RATE_PARTITION));
        sparkConf.set(SPARK_BACK_PRESSURE_ENABLED, applicationProperties.getProperty(SPARK_BACK_PRESSURE_ENABLED));
        sparkConf.set(SPARK_KRYO_REGISTRATOR_CONFIG, applicationProperties.getProperty(SPARK_KRYO_REGISTRATOR_CONFIG));
        sparkConf.set(SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG, applicationProperties.getProperty(SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG));
        sparkConf.set(SPARK_INTERNAL_SERIALIZER_CONFIG, applicationProperties.getProperty(SPARK_INTERNAL_SERIALIZER_CONFIG));
    }
}
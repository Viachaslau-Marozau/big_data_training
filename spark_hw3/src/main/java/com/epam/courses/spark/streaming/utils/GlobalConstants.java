package com.epam.courses.spark.streaming.utils;

public interface GlobalConstants {
    //Global config
    String PROPERTIES_FILE = "conf/config.properties";
    String SERVER_PROPERTIES_FILE = "conf/server.properties";

    //File path
    String INPUT_FILE_PATH = "input_data/viachaslau_marozau/one_device_2015-2017.csv";

    //Generator config
    String GENERATOR_SKIP_HEADER_CONFIG = "skip.header";
    String GENERATOR_SAMPLE_FILE_CONFIG = "sample.file";
    String GENERATOR_BATCH_SLEEP_CONFIG = "batch.sleep";

    //Kafka config
    String KAFKA_RAW_TOPIC_CONFIG = "raw.topic";
    String KAFKA_ENRICHED_TOPIC_CONFIG = "enriched.topic";
    String BATCH_SIZE_CONFIG = "batch.size";
    String USE_KRIO_PRODUCER = "use.krio.producer";
    //Spark config
    String SPARK_MAX_RATE_PARTITION = "spark.streaming.kafka.maxRatePerPartition";
    String SPARK_BACK_PRESSURE_ENABLED = "spark.streaming.backpressure.enabled";
    String SPARK_APP_NAME_CONFIG = "app.name";
    String SPARK_CHECKPOINT_DIR_CONFIG = "checkpoint.dir";
    String SPARK_BATCH_DURATION_CONFIG = "batch.duration";
    String SPARK_CHECKPOINT_INTERVAL_CONFIG = "checkpoint.interval";
    String SPARK_WINDOW_DURATION_CONFIG = "window.duration";
    String SPARK_INTERNAL_SERIALIZER_CONFIG = "spark.serializer";
    String SPARK_KRYO_REGISTRATOR_CONFIG = "spark.kryo.registrator";
    String SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG = "spark.kryo.registrationRequired";
}

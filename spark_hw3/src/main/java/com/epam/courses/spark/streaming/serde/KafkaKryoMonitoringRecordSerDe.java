package com.epam.courses.spark.streaming.serde;

import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaKryoMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaKryoMonitoringRecordSerDe.class);
    private static final int BUFFER_SIZE = 1024;
    ThreadLocal<Kryo> kryoThreadLocal;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        LOGGER.debug("KafkaKryoMonitoringRecordSerDe.configure() called");
        kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        ByteBufferOutput byteBufferOutput = new ByteBufferOutput(BUFFER_SIZE);
        Kryo kryo = kryoThreadLocal.get();
        kryo.writeObject(byteBufferOutput, data);
        return byteBufferOutput.toBytes();
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        Kryo kryo = kryoThreadLocal.get();
        return kryo.readObject(new ByteBufferInput(data), MonitoringRecord.class);
    }

    @Override
    public void close() {
        LOGGER.debug("KafkaKryoMonitoringRecordSerDe.close() called");
        kryoThreadLocal.remove();
    }
}

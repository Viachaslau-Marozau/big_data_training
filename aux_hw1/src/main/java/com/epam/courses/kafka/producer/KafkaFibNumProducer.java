package com.epam.courses.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaFibNumProducer
{
    private static final int TOPIC_INDEX = 0;
    private static final int COUNT_INDEX = 1;

    private static Producer<Long, Long> createProducer()
    {
        Properties props = new Properties();

        props.put("acks", "all");
        props.put("bootstrap.servers", "localhost:9093,localhost:9092");
        props.put("retries", 0);
        props.put("batch.size", 17200);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 36000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        return new KafkaProducer<Long, Long>(props);
    }

    static void runProducer(Producer<Long, Long> producer, String topic, int messageCount)
        throws ExecutionException, InterruptedException
    {
        long time = System.currentTimeMillis();
        long currentFibNum = 0;
        long nextFibNum = 1;
        long tempFibNum;
        try
        {
            for (long index = time; index < time + messageCount; index++)
            {
                final ProducerRecord<Long, Long> record = new ProducerRecord<Long, Long>(topic, index,
                    currentFibNum);

                RecordMetadata metadata = producer.send(record).get();

                tempFibNum = nextFibNum;
                nextFibNum += currentFibNum;
                currentFibNum = tempFibNum;

                long elapsedTime = System.currentTimeMillis() - time;

                System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
            }
        }
        finally
        {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException
    {
        if (args.length != 2)
        {
            System.out.println("Incorrect param length");
            return;
        }

        String topic = args[TOPIC_INDEX];
        int messageCount = Integer.parseInt(args[COUNT_INDEX]);
        Producer<Long, Long> producer = createProducer();
        runProducer(producer, topic, messageCount);
    }
}

package com.epam.courses.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaFibNumConsumer
{
    private static final int TOPIC_INDEX = 0;
    private static final int NUM_INDEX = 1;

    private static Consumer<Long, Long> createConsumer(String topic)
    {
        final Properties props = new Properties();

        props.put("group.id", "test");
        props.put("bootstrap.servers", "localhost:9093,localhost:9092");
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 1000);
        props.put("session.timeout.ms", 30000);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");

        // Create the consumer using props.
        Consumer<Long, Long> consumer = new KafkaConsumer<Long, Long>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    static long runConsumer(Consumer<Long, Long> consumer, int numOutput)
    {
        final int giveUp = 100;
        int noRecordsCount = 0;
        long sum = 0;
        long step = 0;
        while (true)
        {
            final ConsumerRecords<Long, Long> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0)
            {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                    break;
                else
                    continue;
            }
            for (ConsumerRecord<Long, Long> record : consumerRecords)
            {
                sum += record.value();
                step++;
                if (step % numOutput == 0)
                {
                    System.out.printf("Step %s:  sum = %s\n", step, sum);
                }
            }
            consumer.commitAsync();
        }
        consumer.close();
        return sum;
    }

    public static void main(String[] args)
    {
        if (args.length != 2)
        {
            System.out.println("Incorrect param length");
            return;
        }

        String topic = args[TOPIC_INDEX];
        int numOutput = Integer.parseInt(args[NUM_INDEX]);

        Consumer<Long, Long> consumer = createConsumer(topic);
        long sum = runConsumer(consumer, numOutput);
        System.out.printf("Finally:  sum = %s\n", sum);
        System.out.println("DONE");
    }
}

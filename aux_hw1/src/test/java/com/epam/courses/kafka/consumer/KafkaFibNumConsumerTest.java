package com.epam.courses.kafka.consumer;

import junit.framework.Assert;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaFibNumConsumerTest
{
    MockConsumer<Long, Long> consumer;

    @Before
    public void setUp()
    {
        consumer = new MockConsumer<Long, Long>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Arrays.asList(new TopicPartition("test_topic", 0)));
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test_topic", 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
    }

    @Test
    public void testConsumerOneInput() throws Exception
    {
        consumer.addRecord(new ConsumerRecord<Long, Long>("test_topic", 0, 0L, 0L, 7L));

        long summ = KafkaFibNumConsumer.runConsumer(consumer, 10);
        Assert.assertEquals(7L, summ);
    }

    @Test
    public void testConsumerSomeInputs() throws Exception
    {

        consumer.addRecord(new ConsumerRecord<Long, Long>("test_topic", 0, 0L, 0L, 0L));
        consumer.addRecord(new ConsumerRecord<Long, Long>("test_topic", 0, 0L, 1L, 1L));
        consumer.addRecord(new ConsumerRecord<Long, Long>("test_topic", 0, 0L, 2L, 2L));
        consumer.addRecord(new ConsumerRecord<Long, Long>("test_topic", 0, 0L, 3L, 3L));
        consumer.addRecord(new ConsumerRecord<Long, Long>("test_topic", 0, 0L, 5L, 5L));

        long summ = KafkaFibNumConsumer.runConsumer(consumer, 10);
        Assert.assertEquals(11L, summ);
    }

    @Test
    public void testConsumerEmptyInput() throws Exception
    {
        long summ = KafkaFibNumConsumer.runConsumer(consumer, 10);
        Assert.assertEquals(0, summ);
    }
}

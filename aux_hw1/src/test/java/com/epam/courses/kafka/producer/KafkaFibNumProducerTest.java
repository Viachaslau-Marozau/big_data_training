package com.epam.courses.kafka.producer;

import junit.framework.Assert;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KafkaFibNumProducerTest
{
    MockProducer<Long, Long> producer;

    @Before
    public void setUp()
    {
        producer = new MockProducer<Long, Long>(true, new LongSerializer(), new LongSerializer());
    }

    @Test()
    public void testSendZeroMessages() throws ExecutionException, InterruptedException
    {
        KafkaFibNumProducer.runProducer(producer, "test_topic", 0);

        List<ProducerRecord<Long, Long>> history = producer.history();
        List<ProducerRecord<Long, Long>> expected = new ArrayList<>();
        Assert.assertEquals(expected, history);
    }

    @Test()
    public void testSendOneMessage() throws ExecutionException, InterruptedException
    {
        KafkaFibNumProducer.runProducer(producer, "test_topic", 1);

        List<ProducerRecord<Long, Long>> history = producer.history();
        Assert.assertEquals(history.get(0).value(), new Long(0));
    }

    @Test()
    public void testSendFiveMessages() throws ExecutionException, InterruptedException
    {
        KafkaFibNumProducer.runProducer(producer, "test_topic", 5);

        List<ProducerRecord<Long, Long>> history = producer.history();

        Assert.assertEquals(history.get(0).value(), new Long(0));
        Assert.assertEquals(history.get(1).value(), new Long(1));
        Assert.assertEquals(history.get(2).value(), new Long(1));
        Assert.assertEquals(history.get(3).value(), new Long(2));
        Assert.assertEquals(history.get(4).value(), new Long(3));
    }
}

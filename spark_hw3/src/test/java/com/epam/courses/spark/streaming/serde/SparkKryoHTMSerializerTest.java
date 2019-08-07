package com.epam.courses.spark.streaming.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

public class SparkKryoHTMSerializerTest {

    SparkKryoHTMSerializer htmSerializer = null;

    @Before
    public void setUp() {
        htmSerializer = new SparkKryoHTMSerializer();
    }

    @Test()
    public void testSerializeSuccessfullyWrite() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output kryoOutput = new Output(bos);
        Kryo kry = new Kryo();
        try {
            htmSerializer.write(kry, kryoOutput, "test");
        } catch (Exception e) {
            Assert.assertEquals("No exception in this case", e);
        }
    }

    @Test()
    public void testSerializeWriteWithError() {
        Kryo kry = new Kryo();
        try {
            htmSerializer.write(kry, null, "test");
            Assert.assertNull("Expect exception during write object");
        } catch (Exception e) {
            Assert.assertNotNull("Expect exception in this case", e);
        }
    }
}

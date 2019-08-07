package com.epam.courses.spark.streaming.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

// SerDe HTM objects using SerializerCore (https://github.com/RuedigerMoeller/fast-serialization)
public class SparkKryoHTMSerializer<T> extends Serializer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkKryoHTMSerializer.class);
    private static final int BUFFER_SIZE = 4096;

    private final SerializerCore htmSerializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    public SparkKryoHTMSerializer() {
        htmSerializer.registerClass(Network.class);
    }

    @Override
    public T copy(Kryo kryo, T original) {
        return kryo.copy(original);
    }

    @Override
    public void write(Kryo kryo, Output kryoOutput, T t) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream(BUFFER_SIZE);
             HTMObjectOutput writer = htmSerializer.getObjectOutput(kryoOutput);) {
            writer.writeObject(t, t.getClass());
            kryoOutput.writeInt(stream.size());
            stream.writeTo(kryoOutput);
            writer.flush();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    @Override
    public T read(Kryo kryo, Input kryoInput, Class<T> aClass) {
        byte[] data = new byte[kryoInput.readInt()];
        kryoInput.readBytes(data);
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data);
             HTMObjectInput reader = htmSerializer.getObjectInput(stream);) {
            T rObject = (T) reader.readObject(aClass);
            return rObject;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    public static void registerSerializers(Kryo kryo) {
        kryo.register(Network.class, new SparkKryoHTMSerializer<>());
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES)
            kryo.register(c, new SparkKryoHTMSerializer<>());
    }
}
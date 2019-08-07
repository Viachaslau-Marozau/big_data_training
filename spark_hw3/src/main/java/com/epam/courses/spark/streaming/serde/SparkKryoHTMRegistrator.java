package com.epam.courses.spark.streaming.serde;

import com.epam.courses.spark.streaming.htm.HTMNetwork;
import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import com.epam.courses.spark.streaming.htm.ResultState;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkKryoHTMRegistrator implements KryoRegistrator {

    public SparkKryoHTMRegistrator() {
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkKryoHTMRegistrator.class);
    @Override
    public void registerClasses(Kryo kryo) {
        // the rest HTM.java internal classes support Persistence API (with preSerialize/postDeserialize methods),
        // therefore we'll create the seralizers which will use HTMObjectInput/HTMObjectOutput (wrappers on top of fast-serialization)
        // which WILL call the preSerialize/postDeserialize
        SparkKryoHTMSerializer.registerSerializers(kryo);

        try {
            kryo.register(MonitoringRecord.class);
            kryo.register(HTMNetwork.class);
            kryo.register(ResultState.class);
            kryo.register(Long.class);
            kryo.register(Long[].class);
            kryo.register(String.class);
            kryo.register(String[].class);
            kryo.register(Double.class);
            kryo.register(Double[].class);
            kryo.register(int[].class);
            kryo.register(int.class);
            kryo.register(java.util.HashMap.class);
            kryo.register(org.apache.spark.streaming.rdd.MapWithStateRDDRecord.class);
            kryo.register(org.apache.spark.streaming.util.OpenHashMapBasedStateMap.LimitMarker.class);
            kryo.register(org.apache.spark.streaming.util.OpenHashMapBasedStateMap.StateInfo.class);
            kryo.register(org.apache.spark.streaming.util.OpenHashMapBasedStateMap.class);
            kryo.register(org.joda.time.DateTime.class);
            kryo.register(org.joda.time.chrono.ISOChronology.class);
            kryo.register(org.joda.time.chrono.ZonedChronology.class);
            kryo.register(org.joda.time.chrono.GregorianChronology.class);
            kryo.register(org.joda.time.tz.CachedDateTimeZone.class);
            kryo.register(org.joda.time.tz.DateTimeZoneBuilder.class);
            kryo.register(org.numenta.nupic.network.Network.class);
            kryo.register(Class.class);
            kryo.register(Object.class);
            kryo.register(Object[].class);
            kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"));
            kryo.register(Class.forName("scala.reflect.ManifestFactory$$anon$2"));
            kryo.register(Class.forName("org.joda.time.tz.DateTimeZoneBuilder$PrecalculatedZone"));
            kryo.register(Class.forName("org.joda.time.tz.DateTimeZoneBuilder$DSTZone"));
        } catch (ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
        }
    }
}
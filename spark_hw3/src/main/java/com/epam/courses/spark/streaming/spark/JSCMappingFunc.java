package com.epam.courses.spark.streaming.spark;

import com.epam.courses.spark.streaming.htm.HTMNetwork;
import com.epam.courses.spark.streaming.htm.MonitoringRecord;
import com.epam.courses.spark.streaming.htm.ResultState;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.HashMap;

public class JSCMappingFunc implements Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> {


    @Override
    public MonitoringRecord call(String deviceID, Optional<MonitoringRecord> recordOpt, State<HTMNetwork> state) throws Exception {
        // case 0: timeout
        if (!recordOpt.isPresent())
            return null;

        // either new or existing device
        if (!state.exists())
            state.update(new HTMNetwork(deviceID));
        HTMNetwork htmNetwork = state.get();
        String stateDeviceID = htmNetwork.getId();
        if (!stateDeviceID.equals(deviceID))
            throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
        MonitoringRecord record = recordOpt.get();

        // get the value of DT and Measurement and pass it to the HTM
        HashMap<String, Object> m = new java.util.HashMap<>();
        m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
        m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
        ResultState rs = htmNetwork.compute(m);
        record.setPrediction(rs.getPrediction());
        record.setError(rs.getError());
        record.setAnomaly(rs.getAnomaly());
        record.setPredictionNext(rs.getPredictionNext());

        return record;
    }
}

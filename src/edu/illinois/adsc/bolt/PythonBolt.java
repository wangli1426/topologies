package edu.illinois.adsc.bolt;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * Created by Robert on 10/12/15.
 */
public class PythonBolt extends ShellBolt implements IRichBolt {

    public PythonBolt() {
        super("python","processbolt.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("station","time","word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

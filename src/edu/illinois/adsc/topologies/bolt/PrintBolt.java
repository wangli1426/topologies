package edu.illinois.adsc.topologies.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Robert on 10/12/15.
 */
public class PrintBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        System.out.format("station:%s\ttime:%s\tmatrix:%s\n",input.getString(0),input.getString(1),input.getString(2));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

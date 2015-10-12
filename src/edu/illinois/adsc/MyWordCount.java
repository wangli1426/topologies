package topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.scheduler.Topologies;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


import javax.naming.AuthenticationException;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.security.KeyPair;
import java.util.*;

/**
 * Created by robert on 8/18/15.
 */




public class MyWordCount {
    public static class WordGenerationSpout extends BaseRichSpout {
        public WordGenerationSpout(){
            _emit_cycles=1000;
            _random.setSeed(System.currentTimeMillis());
            _dictionary.add("One");
            _dictionary.add("Two");
            _dictionary.add("Three");
            _dictionary.add("Four");
        }
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
            _collector=collector;
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
        @Override
        public void nextTuple(){
            Utils.sleep(_emit_cycles);
            _collector.emit(new Values(_dictionary.get(_random.nextInt(_dictionary.size()))));
        }
        SpoutOutputCollector _collector;
        int _emit_cycles;
        Random _random =new Random();
        Vector<String> _dictionary = new Vector<>();
    }
    public static class MyCounter extends BaseBasicBolt {
        @Override
        public void execute(Tuple input, BasicOutputCollector collector){
            String word = input.getString(0);
            Integer count = _map.get(word);
            if (count == null){
                count = 0;
            }
            count++;
            _map.put(word,count);
            collector.emit(new Values(word,count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("word", "count"));
        }

        public void print(){
            Iterator it = _map.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry) it.next();
                System.out.println("key:" + pair.getKey() + " count:" + pair.getValue());
            }
        }
        public void write(){
            try {
                PrintWriter writer = new PrintWriter("result.output");
                Iterator it = _map.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry pair = (Map.Entry) it.next();
                    writer.println("key:" + pair.getKey() + "count:" + pair.getValue());
                }
                writer.close();
            }
            catch (FileNotFoundException e){
                System.out.print(e.getMessage());
            }



        }

        @Override
        public void cleanup(){
            write();
        }
        Map<String,Integer> _map = new HashMap<String, Integer>();
        Random _random = new Random();
    }

    public static class ReportBolt extends BaseBasicBolt{

        public void execute(Tuple tuple, BasicOutputCollector collector){
            System.out.println("[Result:]" + tuple.getString(0)+"--->"+tuple.getInteger(1));
        }


        public void declareOutputFields(OutputFieldsDeclarer d){
            d.declare(new Fields("result"));
        }
    }

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new WordGenerationSpout(),1);
        builder.setBolt("counter",new MyCounter(),8).fieldsGrouping("spout", new Fields("word"));
        builder.setBolt("printer", new ReportBolt(),1).globalGrouping("counter");


        Config conf = new Config();
        conf.setDebug(false);

        if(args != null && args.length >0){
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (Exception e){
                System.out.print("Error!");
            }
        }
        else {


            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Word-count", conf, builder.createTopology());

            Utils.sleep(10000);
            cluster.shutdown();
        }

    }
}

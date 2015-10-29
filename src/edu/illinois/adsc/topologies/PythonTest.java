package edu.illinois.adsc.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import edu.illinois.adsc.topologies.bolt.PrintBolt;
import edu.illinois.adsc.topologies.bolt.PythonBolt;
import edu.illinois.adsc.topologies.spout.RandomSentenceSpout;

/**
 * Created by Robert on 10/12/15.
 */
public class PythonTest {
    public static void main(String [] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 1);

        builder.setBolt("processbolt",new PythonBolt(),1).shuffleGrouping("spout");

        builder.setBolt("print", new PrintBolt(),1).shuffleGrouping("processbolt");

        Config conf  = new Config();

        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("python test", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }


    }
}

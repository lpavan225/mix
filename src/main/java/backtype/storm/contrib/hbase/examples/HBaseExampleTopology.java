package backtype.storm.contrib.hbase.examples;

import java.util.Arrays;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.hbase.bolts.HBaseBolt;
import backtype.storm.contrib.hbase.bolts.ReportBolt;
import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * An example non-transactional topology that uses the {@link HBaseBolt} to
 * insert a stream of shortened URL's into a HBase table called 'shorturl'.
 * <p>
 * Assumes the HBase table has been created.<br>
 * <tt>create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}</tt>
 */
public class HBaseExampleTopology {
  /**
   * @param args
   */
	  private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
	  private static final String SPLIT_BOLT_ID = "acking-split-bolt";
	  private static final String COUNT_BOLT_ID = "acking-count-bolt";
	  private static final String REPORT_BOLT_ID = "acking-report-bolt";
	  private static final String TOPOLOGY_NAME = "acking-word-count-topology";
	
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
	  
	  

	   /* int numSpoutExecutors = 1;
	    
	    KafkaSpout kspout = buildKafkaSentenceSpout();
	    SplitSentenceBolt splitBolt = new SplitSentenceBolt();
	    WordCountBolt countBolt = new WordCountBolt();
	    ReportBolt reportBolt = new ReportBolt();
	    
	    TopologyBuilder builder = new TopologyBuilder();
	    
	    
	    builder.setSpout(SENTENCE_SPOUT_ID, kspout, numSpoutExecutors);
	    builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
	    builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
	    builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

	    
	    Config cfg = new Config();    
	    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());*/
	  
    TopologyBuilder builder = new TopologyBuilder();
    
    KafkaSpout kspout =  buildKafkaSentenceSpout();
    ReportBolt reportBolt = new ReportBolt();
    // Add test spout
   // builder.setSpout("spout", new TestSpout(), 1);
    
    builder.setSpout(SENTENCE_SPOUT_ID, kspout, 1);

    // Build TupleTableConifg
    TupleTableConfig config = new TupleTableConfig("fuel_dispenser_test", "shortid");
    config.setBatch(false);
    /*config.addColumn("data", "url");
    config.addColumn("data", "user");
    config.addColumn("data", "date");*/
    
    config.addColumn("CF1", "site_id");
    config.addColumn("CF1", "inv_vol");
    config.addColumn("CF1", "inv_tank");
    config.addColumn("CF1", "date");
    config.addColumn("CF1", "refill");
    config.addColumn("CF1", "dispensed");
    
	 builder.setSpout("spout", new TestSpout(), 1);
    builder.setBolt("hbase", new HBaseBolt(config), 1).shuffleGrouping("spout");    
   
    // Add HBaseBolt
	//	builder.setBolt(REPORT_BOLT_ID, reportBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    // builder.setBolt("hbase", new HBaseBolt(config), 1).shuffleGrouping(SENTENCE_SPOUT_ID);
    //builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(SENTENCE_SPOUT_ID);
    System.out.println("inside topo");
    Config stormConf = new Config();
    stormConf.setDebug(true);
    
    //Added 
    /*stormConf.setMaxTaskParallelism(5);
    stormConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
    stormConf.put(Config.NIMBUS_HOST, "52.76.242.227");
    stormConf.put(Config.NIMBUS_THRIFT_PORT, 6627);
    stormConf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
    stormConf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("52.76.242.227"));*/
    Config cfg = new Config();    
    StormSubmitter.submitTopology("hbase-example", cfg, builder.createTopology());
    /*LocalCluster cluster = new LocalCluster();
    cluster
        .submitTopology("hbase-example", stormConf, builder.createTopology());

    Utils.sleep(10000);
    cluster.killTopology("hbase-example");
    cluster.shutdown();*/
  }
  private static KafkaSpout buildKafkaSentenceSpout() {
	    String zkHostPort = "localhost:2181";
	    String topic = "sentences";

	    String zkRoot = "/acking-kafka-sentence-spout";
	    String zkSpoutId = "acking-sentence-spout";
	    ZkHosts zkHosts = new ZkHosts(zkHostPort);
	    
	    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);	    
	    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);   
	    return kafkaSpout;
	  }
  	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    //declarer.declare(new Fields("shortid","site_id", "inv_vol", "inv_tank", "date","refill","dispensed"));
	   // declarer.declare(new Fields("shortid","site_id", "refill" ,"inv_vol", "inv_tank","dispensed","date"));
	    declarer.declare(new Fields("shortid","site_id", "inv_vol" ,"inv_tank", "date","refill","dispensed"));
	  }
}


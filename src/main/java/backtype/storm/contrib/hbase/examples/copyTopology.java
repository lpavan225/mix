package backtype.storm.contrib.hbase.examples;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.hbase.bolts.HBaseBolt;
import backtype.storm.contrib.hbase.bolts.ProcessBolt;
import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
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
public class copyTopology {
  /**
   * @param args
   */
	  private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
	  private static final String SPLIT_BOLT_ID = "acking-split-bolt";
	  private static final String COUNT_BOLT_ID = "acking-count-bolt";
	  private static final String REPORT_BOLT_ID = "acking-report-bolt";
	  private static final String TOPOLOGY_NAME = "acking-word-count-topology";
	
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
	  
	  
	  	String zkHostPort = "localhost:2181";
	   // String zkHostPort = "10.0.0.80:2181";
	    String topic = "sentences";

	    String zkRoot = "/acking-kafka-sentence-spout";
	    String zkSpoutId = "acking-sentence-spout";
	    ZkHosts zkHosts = new ZkHosts(zkHostPort);
	    
	    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);	    
	    //KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);   
	  
	  
    TopologyBuilder builder = new TopologyBuilder();
    
    KafkaSpout kspout =  new BuildKafkaSentenceSpout(spoutCfg);
    //ReportBolt reportBolt = new ReportBolt();
    
    // Build TupleTableConifg
    TupleTableConfig config = new TupleTableConfig("fuel_dispenser_test", "shortid");
    config.setBatch(false);
    
    config.addColumn("CF1", "site_id");
    config.addColumn("CF1", "inv_vol");
    config.addColumn("CF1", "inv_tank");
    config.addColumn("CF1", "date");
    config.addColumn("CF1", "refill");
    config.addColumn("CF1", "dispensed");
    
    //builder.setSpout("spout", new TestSpout(), 1);
    //builder.setBolt("hbase", new HBaseBolt(config), 1).shuffleGrouping("spout");
   
    // Add HBaseBolt
    builder.setSpout(SENTENCE_SPOUT_ID, kspout, 1);
    builder.setBolt("process", new ProcessBolt()).shuffleGrouping(SENTENCE_SPOUT_ID);
    builder.setBolt("hbase", new HBaseBolt(config), 1).fieldsGrouping("process", new Fields("shortid","site_id", "inv_vol" ,"inv_tank", "date","refill","dispensed"));
    //builder.setBolt(REPORT_BOLT_ID, reportBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    
    System.out.println("inside topo");
    Config stormConf = new Config();
    stormConf.setDebug(true);
    
    Config cfg = new Config();    
    StormSubmitter.submitTopology("copyexample", cfg, builder.createTopology());
    
  }

}



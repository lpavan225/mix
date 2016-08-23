package posfeed.inventory.kafka;

import posfeed.inventory.acking.InventoryFileBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import posfeed.inventory.util.PropertyFileLoader;

public class PosFeedAckedTopology {
	private static final String SENTENCE_SPOUT_ID = "kafka-pos-spout";
	private static final String FILE_BOLT_ID = "acking-file-bolt";
	private static final String TOPOLOGY_NAME = "acking-pos-feed-topology";
	
	public static void main(String[] args) throws Exception {
		
		PropertyFileLoader fileLoader = new PropertyFileLoader();
		fileLoader.setConfigLocation("storm-config");
		fileLoader.loadProperties();
		
		int numSpoutExecutors = new Integer(PropertyFileLoader.properties.getProperty("numSpoutExecutors"));
		KafkaSpout kspout = buildKafkaSentenceSpout();
		InventoryFileBolt invBolt = new InventoryFileBolt();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, kspout, numSpoutExecutors);
		builder.setBolt(FILE_BOLT_ID, invBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
		Config cfg = new Config();  
		StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
				
	} //main method
	
	private static KafkaSpout buildKafkaSentenceSpout(){
		//String zkHostPort = "labpc3:2181";
	    String zkHostPort = new String(PropertyFileLoader.properties.getProperty("zkHostPort"));
		String topic = new String(PropertyFileLoader.properties.getProperty("kafkaTopic"));
	    

	    String zkRoot = new String(PropertyFileLoader.properties.getProperty("zkRoot"));
	    String zkSpoutId = new String(PropertyFileLoader.properties.getProperty("zkSpoutId"));
	    ZkHosts zkHosts = new ZkHosts(zkHostPort);
	    
	    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
	    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
	    return kafkaSpout;
	}
	
} // main -class

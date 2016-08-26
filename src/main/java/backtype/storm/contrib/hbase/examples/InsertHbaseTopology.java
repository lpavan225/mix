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
import backtype.storm.contrib.hbase.bolts.ProcessFuelDispenserBolt;
import backtype.storm.contrib.hbase.bolts.ProcessHolidaysBolt;
/**
 * An example non-transactional topology that uses the {@link HBaseBolt} to
 * insert a stream of shortened URL's into a HBase table called 'shorturl'.
 * <p>
 * Assumes the HBase table has been created.<br>
 * <tt>create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}</tt>
 */
public class InsertHbaseTopology {
  /**
   * @param args
   */
	  private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
	 
	
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
	  
	  
	  	String zkHostPort = "localhost:2181";
	  	
	  	//String zkHostPort = "52.76.242.227:2181";
	    String topic = "fuelDispenser";
	    String zkRoot = "/fuelDispenser-spout";
	    String zkSpoutId = "fuelDispenser-spout";
	    ZkHosts zkHosts = new ZkHosts(zkHostPort);
	    
	    
	    String topic1 = "holidays";
	    String zkRoot1 = "/holidays-spout";
	    String zkSpoutId1 = "holidays-spout";
	    
	    SpoutConfig fuelDispenserSpoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);	
	    
	    SpoutConfig holidasySpoutCfg = new SpoutConfig(zkHosts, topic1, zkRoot1, zkSpoutId1);	 
	    //KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);   
	  
	  
    TopologyBuilder fuelDispenserBuilder = new TopologyBuilder();
    
    TopologyBuilder holidayBuilder = new TopologyBuilder();
    
    KafkaSpout fuelDispenserSpout =  new BuildKafkaSentenceSpout(fuelDispenserSpoutCfg);
    KafkaSpout holidaysSpout =  new BuildKafkaSentenceSpout(fuelDispenserSpoutCfg);
    
    //ReportBolt reportBolt = new ReportBolt();
    
    // Build TupleTableConifg
    TupleTableConfig config = new TupleTableConfig("fuel_dispenser_test", "shortid");
    
    TupleTableConfig config1 = new TupleTableConfig("us_holidays_test", "shortid");
    config.setBatch(false);
    config1.setBatch(false);
    
    config.addColumn("CF1", "site_id");
    config.addColumn("CF1", "inv_vol");
    config.addColumn("CF1", "inv_tank");
    config.addColumn("CF1", "date");
    config.addColumn("CF1", "refill");
    config.addColumn("CF1", "dispensed");
    
    
    config1.addColumn("CF1", "datestr");
    config1.addColumn("CF1", "holiday");
    
   // weather data : Date,Precip,Max_Air_Temp,Min_Air_Temp,Avg_Air_Temp
   // us holidays : datestr,holiday
   // fule proces : Datestr,Fuel_Price,State   
   // fuel dispenser : site_id,inv_vol,inv_tank_num,date,refill,dispensed
   
    // Add HBaseBolt
    fuelDispenserBuilder.setSpout(SENTENCE_SPOUT_ID, fuelDispenserSpout, 1);
    fuelDispenserBuilder.setBolt("processFuelDispenser", new ProcessFuelDispenserBolt()).shuffleGrouping(SENTENCE_SPOUT_ID);
    fuelDispenserBuilder.setBolt("fuelDispenserData", new HBaseBolt(config), 1).fieldsGrouping("processFuelDispenser", new Fields("shortid","site_id", "inv_vol" ,"inv_tank", "date","refill","dispensed"));
    //builder.setBolt(REPORT_BOLT_ID, reportBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    
    fuelDispenserBuilder.setBolt("processHoliday", new ProcessHolidaysBolt()).shuffleGrouping(SENTENCE_SPOUT_ID);
    fuelDispenserBuilder.setBolt("holidayData", new HBaseBolt(config1), 1).fieldsGrouping("processHoliday", new Fields("shortid","datestr", "holiday"));
    //builder.setBolt(REPORT_BOLT_ID, reportBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
 
   // Config stormConf = new Config();
   // stormConf.setDebug(true);
    
    Config cfg = new Config();    
    StormSubmitter.submitTopology("multipleTable", cfg, fuelDispenserBuilder.createTopology());
    
  }

}


